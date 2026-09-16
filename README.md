# Minimal reproduction — sub-agent chunk forwarding multiplies published bytes

`@mastra/core@1.67.0`. No Valkey, no Docker: the in-memory `EventEmitterPubSub` is wrapped to
count the bytes each `publish()` would write to a real broker.

```bash
pnpm install
node repro.mjs            # 3 levels (the default)
DEPTH=1 node repro.mjs    # baseline, no delegation
TOOL_CHARS=40000 node repro.mjs
```

## What it builds

A chain of `DEPTH` agents. The leaf calls a tool three times, each returning a ~10k-char result.
Every level above delegates to the level below through the `agents` map, as **plain `Agent`
instances** — the wiring every example in the docs uses. Only the entry agent is durable.

## Result

Same 30,372 B of real tool payload in every row. Only the chain depth changes.

| depth | published to pubsub | × real payload |
| --- | --- | --- |
| 1 (no delegation) | 134,036 B | ×4.4 |
| 2 | 693,628 B | ×22.8 |
| 3 | 1,053,861 B | ×34.7 |
| 4 | 1,418,929 B | ×46.7 |

At depth 3 the largest single published chunk is **160,381 B — 5.3× the entire run's real tool
payload, in one chunk.**

```
by chunk kind (tool-output> prefix = one delegation level):
   160381 B  n=  1  tool-output>tool-output>step-finish [messages.all=9 steps=4]
   160376 B  n=  1  tool-output>tool-output>finish      [messages.all=9 steps=4]
   128008 B  n=  1  tool-output>step-finish             [messages.all=4 steps=2]
   118082 B  n=  1  tool-output>tool-output>step-finish [messages.all=8 steps=3]
    65534 B  n=  1  tool-output>tool-output>step-finish [messages.all=6 steps=2]
```

## What the output shows

`messages.all` and `output.steps` are cumulative, so a chunk grows with the step count and every
emission is published in full. A parent forwards each sub-agent chunk into its own topic wrapped in
a `tool-output` envelope, and the sub-agent's result also enters the parent's message history — so
the parent then re-emits its own enlarged conversation on every parent step. Growth is a product of
depth and step count rather than one copy per level.

`sanitizeBroadcastPart` (added for #21219) strips exactly these fields, but it is applied only to
the thread-stream broadcast and it returns early for `tool-output`, so none of the chunks above are
covered.

Set `DEPTH=1` to see the floor: without delegation the same work publishes 134,036 B.
