# Minimal reproduction — sub-agent chunk forwarding multiplies published bytes

`@mastra/core@1.67.0`. No Valkey, no Docker: the in-memory `EventEmitterPubSub` is wrapped to
count the bytes each `publish()` would write to a real broker.

```bash
npm i
npm run repro             # 3 levels (the default)
npm run sweep             # depths 1..4 in one table
DEPTH=1 node repro.mjs    # baseline, no delegation
TOOL_CHARS=40000 node repro.mjs
```

Everything is stock `@mastra/core`: `EventEmitterPubSub`, `InMemoryServerCache`,
`MockLanguageModelV3`, plain `Agent` instances. No third-party transport, no custom wrapper, no
application code.

## What it builds

A chain of `DEPTH` agents. The leaf calls a tool three times, each returning a ~10k-char result.
Every level above delegates to the level below through the `agents` map, as **plain `Agent`
instances** — the wiring every example in the docs uses. Only the entry agent is durable.

## Result

Same 30,372 B of real tool payload in every row. Only the chain depth changes.

| depth | published to pubsub | × real payload |
| --- | --- | --- |
| 1 (no delegation) | 205,805 B | ×6.8 |
| 2 | 1,369,140 B | ×45.1 |
| 3 | 2,695,194 B | ×88.7 |
| 4 | 4,396,498 B | ×144.8 |

### The sanitizer gap, on the one topic it is applied to

`sanitizeBroadcastPart` runs on the thread-stream broadcast. On that same topic, for the same
chunk type, a top-level part is stripped and a nested one is not:

```
sanitizeBroadcastPart is applied to this topic. Same topic, same chunk type:
  top-level step-finish  n=  8  avg        4933 B   sanitised (no messages, no output.steps)
  nested    step-finish  n=  1           160470 B   NOT sanitised — [messages.all=9 steps=4]
  ratio                                  x33
```

A forwarded sub-agent chunk arrives as `{ type: "tool-output", payload: { output: <chunk> } }`.
`sanitizeBroadcastPart` matches on the outer `type`, sees `tool-output`, and returns the part
untouched — once per delegation level.

## What the output shows

`messages.all` and `output.steps` are cumulative, so a chunk grows with the step count and every
emission is published in full. A parent forwards each sub-agent chunk into its own topic wrapped in
a `tool-output` envelope, and the sub-agent's result also enters the parent's message history — so
the parent then re-emits its own enlarged conversation on every parent step. Growth is a product of
depth and step count rather than one copy per level.

`sanitizeBroadcastPart` (added for #21219) strips exactly these fields, but it is applied only to
the thread-stream broadcast and it returns early for `tool-output`, so none of the chunks above are
covered.

Set `DEPTH=1` to see the floor: without delegation the same work publishes 134,113 B.

Measured on `@mastra/core@1.67.0`, Node 24. 1.65.0 gives the same figures within ~80 bytes.
