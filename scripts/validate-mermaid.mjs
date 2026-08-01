/**
 * Parses every ```mermaid block in the repository's Markdown and fails on the first
 * one that does not.
 *
 * This exists because "the diagrams render on GitHub" has been broken three times:
 * twice while the diagrams were being written, and once by a search-and-replace that
 * inserted backticks into a node label. Every time, the repository looked fine
 * locally and GitHub showed "Unable to render rich display" on the rendered page -
 * a failure visible only to someone browsing the repo, which is exactly the audience
 * these diagrams exist for.
 *
 * Uses mermaid's own parser rather than a renderer: a headless browser is the slow,
 * fragile part of diagram tooling, and parsing is what catches the whole class of
 * defect seen so far. It will not catch a diagram that parses but lays out badly.
 */
import { readFileSync } from "fs";
import { execSync } from "child_process";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><body></body>", { pretendToBeVisual: true });

// Assigned through defineProperty rather than `global.x = y`. On Node 22+ some of
// these - `navigator` in particular - are getter-only on globalThis, so a plain
// assignment throws `Cannot set property navigator of #<Object> which has only a
// getter`. Node 20 allows it, which is exactly how this shipped broken: it was
// tested locally on 20 and CI runs 22.
for (const [name, value] of [
  ["window", dom.window],
  ["document", dom.window.document],
  ["navigator", dom.window.navigator],
  ["Element", dom.window.Element],
]) {
  Object.defineProperty(globalThis, name, { value, configurable: true, writable: true });
}

const mermaid = (await import("mermaid")).default;
mermaid.initialize({ startOnLoad: false, securityLevel: "loose" });

const files = execSync("git ls-files '*.md'", { encoding: "utf8" })
  .split("\n")
  .filter(Boolean);

let checked = 0;
let failed = 0;

for (const file of files) {
  const lines = readFileSync(file, "utf8").split("\n");
  let inBlock = false;
  let startLine = 0;
  let buf = [];

  for (let i = 0; i < lines.length; i++) {
    if (!inBlock && lines[i].startsWith("```mermaid")) {
      inBlock = true;
      startLine = i + 2; // 1-indexed, first line after the fence
      buf = [];
    } else if (inBlock && lines[i].startsWith("```")) {
      inBlock = false;
      checked++;
      try {
        await mermaid.parse(buf.join("\n"));
      } catch (err) {
        failed++;
        const message = String(err.message).split("\n")[0];
        console.error(`FAIL ${file} (block starting line ${startLine}): ${message}`);
      }
    } else if (inBlock) {
      buf.push(lines[i]);
    }
  }
}

console.log(`${checked} mermaid diagram(s) checked, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
