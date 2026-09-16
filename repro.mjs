// Minimal reproduction: sub-agent chunk forwarding multiplies published bytes.
//
//   node repro.mjs            # default: 3 levels, 10k-char tool results
//   DEPTH=1 node repro.mjs    # no delegation, for the baseline
//   DEPTH=2 node repro.mjs
//   TOOL_CHARS=40000 node repro.mjs
//
// No Valkey, no Docker: the in-memory EventEmitterPubSub is wrapped to count the bytes
// each publish would write to a real broker.

import { Agent } from "@mastra/core/agent";
import { createEventedAgent } from "@mastra/core/agent/durable";
import { EventEmitterPubSub } from "@mastra/core/events";
import { InMemoryServerCache } from "@mastra/core/cache";
import { Mastra } from "@mastra/core/mastra";
import { createTool } from "@mastra/core/tools";
import { InMemoryStore } from "@mastra/core/storage";
import { Memory } from "@mastra/memory";
import { MockLanguageModelV3, simulateReadableStream } from "ai/test";
import { randomUUID } from "node:crypto";
import { z } from "zod";

const DEPTH = Number(process.env.DEPTH ?? 3);          // 1 = no delegation
const TOOL_CHARS = Number(process.env.TOOL_CHARS ?? 10_000);
const TOOL_CALLS = Number(process.env.TOOL_CALLS ?? 3); // tool calls made by the leaf agent

// ---------------------------------------------------------------- measurement

let realToolBytes = 0;
const byTopic = new Map();
const byKind = new Map();
// Same chunk kinds, but split by which topic family they were published to, so the
// thread-stream broadcast (the path sanitizeBroadcastPart covers) can be read on its own.
const byFamily = { "thread-stream broadcast": new Map(), "run stream": new Map() };
let publishedTotal = 0;
let largestChunk = { bytes: 0, label: "-" };

// Describe a chunk by its nesting depth and the payload it carries.
const describe = (chunk, fallback = "?") => {
  let cur = chunk, depth = 0;
  while (cur?.type === "tool-output" && depth < 8) { cur = cur.payload?.output; depth++; }
  const msgs = cur?.payload?.messages?.all?.length ?? 0;
  const steps = cur?.payload?.output?.steps?.length ?? 0;
  const carries = msgs || steps ? ` [messages.all=${msgs} steps=${steps}]` : "";
  return `${"tool-output>".repeat(depth)}${cur?.type ?? fallback}${carries}`;
};

class MeasuringPubSub extends EventEmitterPubSub {
  async publish(topic, event, options) {
    const bytes = Buffer.byteLength(JSON.stringify(event));
    publishedTotal += bytes;
    const topicKey = topic.replace(/[0-9a-f-]{36}/gi, "<id>");
    byTopic.set(topicKey, (byTopic.get(topicKey) ?? 0) + bytes);
    // run stream publishes { type, data: <chunk> }; the thread-stream broadcast wraps the
    // chunk one level deeper, as a "stream-part" envelope carrying it under .part.
    let chunk = event?.data ?? event;
    if (chunk?.type === "stream-part") chunk = chunk.part ?? chunk;
    if (chunk?.type || event?.type === "finish") {
      const label = describe(chunk ?? event, event?.type ?? "unknown");
      const prev = byKind.get(label) ?? { n: 0, bytes: 0 };
      byKind.set(label, { n: prev.n + 1, bytes: prev.bytes + bytes });
      const family = topic.includes("thread-stream") ? "thread-stream broadcast" : "run stream";
      const fam = byFamily[family];
      if (fam) {
        const fp = fam.get(label) ?? { n: 0, bytes: 0 };
        fam.set(label, { n: fp.n + 1, bytes: fp.bytes + bytes });
      }
      if (bytes > largestChunk.bytes) largestChunk = { bytes, label };
    }
    return super.publish(topic, event, options);
  }
}

// ---------------------------------------------------------------- the chain

const usage = {
  inputTokens: { total: 1, noCache: 1, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 1, text: 1, reasoning: 0 },
};

// A tool result shaped like a real API page, grown to `TOOL_CHARS`.
const toolPayload = (seed) => {
  const items = [];
  for (let i = 1, size = 0; size < TOOL_CHARS; i++) {
    const item = {
      id: `${seed}.${i}`,
      title: `Record ${i} for query ${seed}`,
      body: `Long descriptive text for record ${i}. `.repeat(4),
      quantity: i * 1.5,
      unit: "m2",
    };
    size += Buffer.byteLength(JSON.stringify(item));
    items.push(item);
  }
  return { page: 1, items };
};

const scriptedModel = (script) => {
  let call = 0;
  return new MockLanguageModelV3({
    doStream: async () => {
      const step = script[Math.min(call, script.length - 1)];
      call++;
      const chunks = step.text
        ? [
            { type: "stream-start", warnings: [] },
            { type: "text-start", id: "0" },
            { type: "text-delta", id: "0", delta: step.text },
            { type: "text-end", id: "0" },
            { type: "finish", finishReason: { unified: "stop", raw: "stop" }, usage },
          ]
        : [
            { type: "stream-start", warnings: [] },
            { type: "tool-call", toolCallId: `call-${call}`, toolName: step.tool, input: JSON.stringify(step.input) },
            { type: "finish", finishReason: { unified: "tool-calls", raw: "tool_use" }, usage },
          ];
      return { stream: simulateReadableStream({ chunks }) };
    },
    doGenerate: async () => ({
      content: [{ type: "text", text: "done" }],
      finishReason: { unified: "stop", raw: "stop" },
      usage,
      warnings: [],
    }),
  });
};

const searchTool = createTool({
  id: "search",
  description: "search",
  inputSchema: z.object({}).passthrough(),
  execute: async (input) => {
    const result = toolPayload(input?.seed ?? 1);
    realToolBytes += Buffer.byteLength(JSON.stringify(result));
    return result;
  },
});

const pubsub = new MeasuringPubSub();
const mastra = new Mastra({ pubsub, cache: new InMemoryServerCache(), logger: false });

// Leaf agent: calls the tool TOOL_CALLS times, then answers.
const leafScript = [
  ...Array.from({ length: TOOL_CALLS }, (_, i) => ({ tool: "search", input: { seed: i + 1 } })),
  { text: "Leaf finished its work." },
];
let child = new Agent({
  id: `level-${DEPTH}`,
  name: `level-${DEPTH}`,
  instructions: "leaf",
  model: scriptedModel(leafScript),
  defaultOptions: { maxSteps: 12 },
  tools: { search: searchTool },
  ...(DEPTH === 1 ? { memory: new Memory({ storage: new InMemoryStore(), options: { generateTitle: false } }) } : {}),
});

// Each level above delegates to the level below. Sub-agents are plain Agent instances,
// exactly as every example in the docs shows.
for (let level = DEPTH - 1; level >= 1; level--) {
  const childRef = child;
  const childKey = `child`;
  child = new Agent({
    id: `level-${level}`,
    name: `level-${level}`,
    instructions: "supervisor",
    model: scriptedModel([
      { tool: `agent-${childKey}`, input: { prompt: "Do your part." } },
      { text: `Level ${level} finished.` },
    ]),
    defaultOptions: { maxSteps: 12 },
    agents: () => ({ [childKey]: childRef }),
    // The entry agent needs memory for the thread-stream broadcast to run at all.
    ...(level === 1 ? { memory: new Memory({ storage: new InMemoryStore(), options: { generateTitle: false } }) } : {}),
  });
}

mastra.addAgent(createEventedAgent({ agent: child }));
const entry = mastra.getAgent(`level-1`);
await mastra.startWorkers();

let clientBytes = 0, clientChunks = 0;
const threadId = randomUUID();
const run = await entry.stream("Start the job.", {
  maxSteps: 12,
  memory: { thread: threadId, resource: "repro-user" },
});
for await (const chunk of run.output.fullStream) {
  clientBytes += Buffer.byteLength(JSON.stringify(chunk));
  clientChunks++;
}

// ---------------------------------------------------------------- report

const pad = (n) => String(n).padStart(11);
console.log(`\ndepth=${DEPTH}  toolChars=${TOOL_CHARS}  toolCalls=${TOOL_CALLS}`);
console.log(`real tool payload      ${pad(realToolBytes)} B`);
console.log(`published to pubsub    ${pad(publishedTotal)} B   x${(publishedTotal / realToolBytes).toFixed(1)}`);
console.log(`streamed to the client ${pad(clientBytes)} B   x${(clientBytes / realToolBytes).toFixed(1)}  (${clientChunks} chunks)`);

console.log(`\nby topic:`);
for (const [t, b] of [...byTopic].sort((a, b2) => b2[1] - a[1])) console.log(`  ${pad(b)} B  ${t}`);

console.log(`\nby chunk kind (tool-output> prefix = one delegation level):`);
for (const [k, v] of [...byKind].sort((a, b) => b[1].bytes - a[1].bytes).slice(0, 12)) {
  console.log(`  ${pad(v.bytes)} B  n=${String(v.n).padStart(3)}  ${k}`);
}

for (const [family, map] of Object.entries(byFamily)) {
  const rows = [...map].filter(([k]) => /step-finish|finish/.test(k)).sort((a, b) => b[1].bytes - a[1].bytes);
  if (!rows.length) continue;
  console.log(`\n${family} — step-finish / finish parts only:`);
  for (const [k, v] of rows.slice(0, 20)) console.log(`  ${pad(v.bytes)} B  n=${String(v.n).padStart(3)}  ${k}`);
}

// The headline: on the ONE topic sanitizeBroadcastPart is applied to, compare a top-level
// step-finish (sanitised) against a nested one (not sanitised).
{
  const fam = byFamily["thread-stream broadcast"];
  const flat = [...fam].filter(([k]) => /^step-finish/.test(k));
  const nested = [...fam].filter(([k]) => /^tool-output.*step-finish/.test(k)).sort((a, b) => b[1].bytes - a[1].bytes)[0];
  if (flat.length && nested) {
    const flatN = flat.reduce((n, [, v]) => n + v.n, 0);
    const flatB = flat.reduce((n, [, v]) => n + v.bytes, 0);
    console.log(`\nsanitizeBroadcastPart is applied to this topic. Same topic, same chunk type:`);
    console.log(`  top-level step-finish  n=${String(flatN).padStart(3)}  avg ${pad(Math.round(flatB / flatN))} B   sanitised (no messages, no output.steps)`);
    console.log(`  nested    step-finish  n=${String(nested[1].n).padStart(3)}      ${pad(nested[1].bytes)} B   NOT sanitised — ${nested[0].match(/\[.*\]/)?.[0] ?? ""}`);
    console.log(`  ratio                                  x${(nested[1].bytes / (flatB / flatN)).toFixed(0)}`);
  }
}

console.log(`\nlargest single chunk   ${pad(largestChunk.bytes)} B  = ${(largestChunk.bytes / realToolBytes).toFixed(1)}x the whole run's real tool payload`);
console.log(`  ${largestChunk.label}\n`);
