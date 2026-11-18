import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import type { CompiledWorkflowFn } from '../src/runtime/workflow-registry';

async function withGeneratedCode(
  onError: string | undefined,
  check: (compile: ReturnType<typeof Bun.spawnSync>, load: () => Promise<CompiledWorkflowFn>) => Promise<void>,
  mode?: string,
) {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-code-error-'));
  try {
    const input = path.join(dir, 'records.json');
    const output = path.join(dir, 'service');
    writeFileSync(input, JSON.stringify({
      nodes: [
        {
          name: 'Source', type: 'n8n-nodes-base.code', typeVersion: 2, onError,
          parameters: {
            mode, jsCode: 'if ($json.shouldThrow) throw new Error("fixture failure");\nreturn [{ json: { text: `line one\nline two` } }];',
          },
        },
        {
          name: 'Next', type: 'n8n-nodes-base.code', typeVersion: 2,
          parameters: { jsCode: 'return $input.all().map(item => ({ json: { ...item.json, next: true } }));' },
        },
      ],
      connections: { Source: { main: [[{ node: 'Next', type: 'main', index: 0 }]] } },
    }));
    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', output, '--strict',
    ]);
    await check(compile, async () => (await import(path.join(output, 'workflows', 'records.ts'))).workflow_records);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('continueRegularOutput forwards one error item and keeps successful Code behavior', async () => {
  let defaultOutput: unknown;
  await withGeneratedCode(undefined, async (compile, load) => {
    expect(compile.exitCode).toBe(0);
    const run = await load();
    defaultOutput = (await run([{ json: { shouldThrow: false } }])).items;
    await expect(run([{ json: { shouldThrow: true } }])).rejects.toThrow('fixture failure');
  });
  await withGeneratedCode('continueRegularOutput', async (compile, load) => {
    expect(compile.exitCode).toBe(0);
    const run = await load();
    const success = await run([{ json: { shouldThrow: false } }]);
    expect(success.items).toEqual(defaultOutput);
    const failure = await run([{ json: { shouldThrow: true } }, { json: { shouldThrow: false } }]);
    expect(failure.items).toEqual([{ json: { error: 'fixture failure', next: true } }]);
    expect(failure.context.getNode('Source').all()).toEqual([{ json: { error: 'fixture failure' } }]);
  });
});

test.each([
  ['continueErrorOutput', 'runOnceForAllItems'],
  ['continueRegularOutput', 'runOnceForEachItem'],
])('strict mode reports unsupported Code error settings %s / %s', async (onError, mode) => {
  await withGeneratedCode(onError, async (compile) => {
    expect(compile.exitCode).toBe(1);
    expect(compile.stderr.toString()).toContain('the configured onError behavior is ignored');
  }, mode);
});
