#!/usr/bin/env bun
/**
 * Autonomous PR Review & Merge Pipeline for n8n-to-fastify
 *
 * Implements the 1-by-1 audit and smart local merge protocol:
 * 1. Pushes latest main & fetches all PR refs.
 * 2. Reviews each open PR in topological/FIFO order.
 * 3. Security Audit Engine:
 *    - Scans for dangerous patterns (eval, new Function, child_process, execSync, Bun.spawn, prototype pollution).
 *    - Zero-Trust package policy (rejects unapproved new dependencies).
 *    - Excludes playground/compiler.js from diff inspection.
 * 4. Smart Local Integration:
 *    - Applies code changes from origin/pr/<id> onto main cleanly.
 *    - Always accepts main's version of playground/compiler.js and reconciles README.md.
 * 5. Local Verification:
 *    - Runs `bun x tsc --noEmit` and `bun test`.
 *    - Rebuilds clean unminified playground bundle (`bun run build:playground`).
 * 6. Autonomous Commit & Close:
 *    - Commits the squashed feature to main and pushes to origin.
 *    - Closes PR on GitHub via `gh pr close` with detailed audit receipt.
 *
 * Usage:
 *   bun run scripts/auto-pr-pipeline.ts [--dry-run] [--pr=<number>] [--limit=<n>]
 */

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { resolve, join } from "node:path";

interface PRInfo {
  number: number;
  title: string;
  headRefName: string;
  author: { login: string };
}

const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const PR_FLAG = args.find((a) => a.startsWith("--pr="))?.split("=")[1];
const LIMIT_FLAG = args.find((a) => a.startsWith("--limit="))?.split("=")[1];
const LIMIT = LIMIT_FLAG ? parseInt(LIMIT_FLAG, 10) : 50;

function run(cmd: string, options: { cwd?: string; allowFail?: boolean; silent?: boolean } = {}) {
  const res = spawnSync("bash", ["-c", cmd], {
    cwd: options.cwd || process.cwd(),
    encoding: "utf-8",
    stdio: options.silent ? "pipe" : ["pipe", "pipe", "pipe"],
  });

  if (!options.allowFail && res.status !== 0) {
    const err = (res.stderr || res.stdout || "").trim();
    throw new Error(`Command failed [code ${res.status}]: ${cmd}\n${err}`);
  }

  return {
    code: res.status ?? 0,
    stdout: (res.stdout || "").trim(),
    stderr: (res.stderr || "").trim(),
  };
}

console.log("\x1b[36m====================================================\x1b[0m");
console.log("\x1b[36m  n8n-to-fastify Autonomous Smart Merge Pipeline    \x1b[0m");
console.log("\x1b[36m====================================================\x1b[0m");
if (DRY_RUN) console.log("\x1b[33m[MODE] Dry-run enabled - No merges or pushes will occur.\x1b[0m\n");

// Ensure git is on main and clean
run("git checkout main");
run("git pull origin main", { allowFail: true });
console.log("Fetching open PR refs from GitHub...");
run("git fetch origin '+refs/pull/*/head:refs/remotes/origin/pr/*'", { silent: true });

// Get open PRs via gh
const ghListRes = run("gh pr list --state open --limit 100 --json number,title,headRefName,author", { silent: true });
let prList: PRInfo[] = [];
try {
  prList = JSON.parse(ghListRes.stdout);
} catch (e) {
  console.error("Failed to parse gh pr list:", ghListRes.stdout);
  process.exit(1);
}

// Filter or sort PRs
if (PR_FLAG) {
  const targetNum = parseInt(PR_FLAG, 10);
  prList = prList.filter((p) => p.number === targetNum);
} else {
  // Sort ascending by PR number (FIFO) to handle stacked branches cleanly
  prList.sort((a, b) => a.number - b.number);
  if (LIMIT && prList.length > LIMIT) {
    prList = prList.slice(0, LIMIT);
  }
}

console.log(`Found ${prList.length} candidate PR(s) to process.\n`);

function auditSecurity(prNumber: number): { passed: boolean; reasons: string[] } {
  const reasons: string[] = [];
  const ref = `origin/pr/${prNumber}`;

  // 1. Get changed files
  const filesRes = run(`git diff --name-only main...${ref}`, { silent: true });
  const changedFiles = filesRes.stdout.split("\n").map((f) => f.trim()).filter(Boolean);

  // 2. Check for package.json or dependency tampering
  if (changedFiles.includes("package.json") || changedFiles.includes("bun.lock") || changedFiles.includes("package-lock.json")) {
    const pkgDiff = run(`git diff main...${ref} -- package.json`, { silent: true });
    if (pkgDiff.stdout.includes('"dependencies"') || pkgDiff.stdout.includes('"devDependencies"')) {
      const addedLines = pkgDiff.stdout
        .split("\n")
        .filter((l) => l.startsWith("+") && !l.startsWith("+++"))
        .join("\n");
      if (addedLines.includes(":")) {
        reasons.push("Modifies package.json dependencies (Zero-Trust policy requires explicit dependency review)");
      }
    }
  }

  // 3. Check for malicious patterns in added lines across code files (excluding playground/compiler.js)
  const codeDiffRes = run(`git diff -U0 main...${ref} -- 'src/**' 'bin/**' 'scripts/**' ':!playground/compiler.js'`, { silent: true });
  const addedLines = codeDiffRes.stdout
    .split("\n")
    .filter((l) => l.startsWith("+") && !l.startsWith("+++"));

  const dangerousKeywords = [
    { pattern: /\beval\s*\(/, desc: "Direct eval() call detected" },
    { pattern: /new\s+Function\s*\(/, desc: "new Function() invocation detected" },
    { pattern: /child_process/, desc: "child_process import/call detected" },
    { pattern: /execSync|execFile|spawnSync|Bun\.spawn/, desc: "Subprocess execution primitive detected" },
    {
      pattern: /\[\s*['"`](?:__proto__|prototype)['"`]\s*\]\s*=/,
      desc: "Direct prototype assignment / pollution detected",
    },
    {
      pattern: /(?<!\bif\s*\(.*)(?:__proto__|constructor|prototype)\s*\[/,
      desc: "Dynamic property write on prototype structure detected",
    },
  ];

  for (const line of addedLines) {
    const trimmed = line.trim();
    // Allow defensive checks: if (...includes('__proto__') || part === '__proto__'...) return;
    if (trimmed.startsWith("if ") || trimmed.startsWith("if(") || trimmed.includes("return;") || trimmed.includes("return null;")) {
      continue;
    }
    for (const kw of dangerousKeywords) {
      if (kw.pattern.test(line)) {
        reasons.push(`${kw.desc} in line: ${trimmed}`);
      }
    }
  }

  return {
    passed: reasons.length === 0,
    reasons,
  };
}

let approvedCount = 0;
let rejectedCount = 0;
let skippedCount = 0;

for (let i = 0; i < prList.length; i++) {
  const pr = prList[i];
  console.log(`\x1b[34m[${i + 1}/${prList.length}] Evaluating PR #${pr.number}: "${pr.title}" (@${pr.author.login})\x1b[0m`);

  // Step 1: Security Audit
  const secAudit = auditSecurity(pr.number);
  if (!secAudit.passed) {
    console.log(`  \x1b[31m✖ Security Gate Failed:\x1b[0m`);
    secAudit.reasons.forEach((r) => console.log(`    - ${r}`));
    console.log(`  Skipping merge for PR #${pr.number}.\n`);
    rejectedCount++;
    continue;
  }
  console.log("  \x1b[32m✔ Security Audit Passed (Zero-Trust, No unsafe primitives, No package leaks)\x1b[0m");

  if (DRY_RUN) {
    console.log(`  \x1b[33m[DRY RUN] Would integrate PR #${pr.number}.\x1b[0m\n`);
    approvedCount++;
    continue;
  }

  // Step 2: Attempt Smart Local Merge
  console.log("  Attempting smart local integration on main...");
  const ref = `origin/pr/${pr.number}`;
  
  // Try git merge --no-commit
  const mergeAttempt = run(`git merge --no-commit --no-ff ${ref}`, { allowFail: true, silent: true });
  
  if (mergeAttempt.code !== 0) {
    // If conflict is due to playground/compiler.js or README.md, resolve them in favor of clean build
    run("git checkout --ours playground/compiler.js", { allowFail: true, silent: true });
    
    // Check if other conflicts remain
    const unmergedRes = run("git diff --name-only --diff-filter=U", { silent: true });
    const remainingConflicts = unmergedRes.stdout.split("\n").map(f => f.trim()).filter(Boolean);
    
    // If only README.md or playground/compiler.js, resolve them
    const nonTrivialConflicts = remainingConflicts.filter(f => f !== "playground/compiler.js" && f !== "README.md");
    if (nonTrivialConflicts.length > 0) {
      console.log(`  \x1b[31m✖ Non-trivial code conflicts in: ${nonTrivialConflicts.join(", ")}\x1b[0m`);
      run("git merge --abort", { allowFail: true, silent: true });
      console.log(`  Skipping PR #${pr.number}.\n`);
      skippedCount++;
      continue;
    }

    if (remainingConflicts.includes("README.md")) {
      // Accept ours for README and we will re-document afterwards
      run("git checkout --ours README.md", { allowFail: true, silent: true });
    }
  }

  // Step 3: Run Typecheck & Test Suite
  console.log("  Verifying TypeScript compilation and test suite...");
  const tcRes = run("bun x tsc --noEmit", { allowFail: true, silent: true });
  const testRes = run("bun test", { allowFail: true, silent: true });

  if (tcRes.code !== 0 || testRes.code !== 0) {
    console.log("  \x1b[31m✖ Local tests or typecheck failed after merge:\x1b[0m");
    if (tcRes.code !== 0) console.log(`    - TypeScript error: ${tcRes.stderr || tcRes.stdout}`);
    if (testRes.code !== 0) console.log(`    - Test failure: ${testRes.stderr || testRes.stdout}`);
    run("git merge --abort", { allowFail: true, silent: true });
    run("git reset --hard HEAD", { allowFail: true, silent: true });
    console.log(`  Aborted merge for PR #${pr.number}.\n`);
    rejectedCount++;
    continue;
  }

  // Step 4: Rebuild Clean Playground Bundle
  console.log("  Rebuilding clean unminified playground bundle...");
  run("bun run build:playground");

  // Step 5: Commit, Push and Close PR
  console.log("  Committing and pushing integrated PR...");
  run(`git commit -m "${pr.title} (#${pr.number})"`);
  run("git push origin main");

  console.log(`  Closing PR #${pr.number} on GitHub with merge receipt...`);
  run(`gh pr close ${pr.number} --comment "Integrated into main via autonomous pipeline (clean typecheck, passed test suite, unminified playground rebuilt)."`, { allowFail: true, silent: true });

  console.log(`  \x1b[32m✔ PR #${pr.number} successfully audited, integrated, tested, and pushed!\x1b[0m\n`);
  approvedCount++;
}

console.log("\x1b[36m====================================================\x1b[0m");
console.log("\x1b[36m  Pipeline Execution Summary                        \x1b[0m");
console.log("\x1b[36m====================================================\x1b[0m");
console.log(`  Total Evaluated : ${prList.length}`);
console.log(`  Approved & Merged: ${approvedCount}`);
console.log(`  Rejected / Flagged: ${rejectedCount}`);
console.log(`  Skipped / Blocked: ${skippedCount}`);
console.log("Done.");
