// Runs repro.mjs at depths 1..4 and prints one summary table.
import { execFileSync } from "node:child_process";

const rows = [];
for (const depth of [1, 2, 3, 4]) {
  const out = execFileSync(process.execPath, ["repro.mjs"], {
    env: { ...process.env, DEPTH: String(depth) },
    encoding: "utf8",
  });
  const real = Number(/real tool payload\s+(\d+) B/.exec(out)?.[1]);
  const pub = Number(/published to pubsub\s+(\d+) B/.exec(out)?.[1]);
  rows.push({ depth, real, pub, ratio: pub / real });
}

console.log(`\nsame payload at every depth — only the chain depth changes\n`);
console.log(`depth   real payload   published to pubsub   multiple`);
for (const r of rows) {
  console.log(
    `${String(r.depth).padStart(5)}   ${String(r.real).padStart(12)}   ${String(r.pub).padStart(19)}   x${r.ratio.toFixed(1)}`,
  );
}
console.log();
