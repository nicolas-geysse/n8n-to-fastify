import { expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { getWaitDelayMs } from '../src/runtime/wait-interval';

function compileWait(dir: string, typeVersion: number, parameters: Record<string, unknown>) {
  const input = path.join(dir, 'wait.json');
  const output = path.join(dir, 'service');
  writeFileSync(input, JSON.stringify({
    nodes: [
      {
        name: 'Prepare', type: 'n8n-nodes-base.code', parameters: {
          jsCode: 'return $input.all().map(item => ({ ...item, json: { delay: item.json.delayMs / 1000, unit: "seconds" } }));',
        },
      },
      { name: 'Wait', type: 'n8n-nodes-base.wait', typeVersion, parameters },
      { name: 'Finish', type: 'n8n-nodes-base.code', parameters: { jsCode: 'return $input.all();' } },
    ],
    connections: {
      Prepare: { main: [[{ node: 'Wait', type: 'main', index: 0 }]] },
      Wait: { main: [[{ node: 'Finish', type: 'main', index: 0 }]] },
    },
  }));
  const result = Bun.spawnSync([
    process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', output, '--strict',
  ]);
  return { result, workflow: path.join(output, 'workflows', 'wait.ts') };
}

test('compiled Wait evaluates parameters on the first item of the current batch and preserves the batch', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-wait-'));
  try {
    const { result, workflow } = compileWait(dir, 1.1, {
      amount: '={{ $json.delay }}', unit: '={{ $input.all()[0].json.unit }}',
    });
    expect(result.exitCode).toBe(0);
    const { workflow_wait } = await import(workflow);
    const input = [{ json: { delayMs: 0 }, binary: { fixture: { data: 'AA==' } } }, { json: { delayMs: -2 } }];
    expect((await workflow_wait(input)).items).toEqual([
      { json: { delay: 0, unit: 'seconds' }, binary: input[0].binary },
      { json: { delay: -0.002, unit: 'seconds' } },
    ]);
    await expect(workflow_wait([{ json: { delayMs: -1 } }])).rejects.toThrow('Wait amount');
    expect((await workflow_wait([])).items).toEqual([]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test.each([
  [1, 'waitForInterval(1, "hours")'],
  [1.1, 'waitForInterval(5, "seconds")'],
])('Wait v%s compiles the n8n defaults', (version, expected) => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-wait-defaults-'));
  try {
    const { result, workflow } = compileWait(dir, version, {});
    expect(result.exitCode).toBe(0);
    expect(readFileSync(workflow, 'utf8')).toContain(expected);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('timed Wait converts zero, fractional, and day intervals', () => {
  expect(getWaitDelayMs(0, 'seconds')).toBe(0);
  expect(getWaitDelayMs(0.5, 'seconds')).toBe(500);
  expect(getWaitDelayMs(1, 'minutes')).toBe(60_000);
  expect(getWaitDelayMs(2, 'hours')).toBe(7_200_000);
  expect(getWaitDelayMs(1, 'days')).toBe(86_400_000);
  expect(() => getWaitDelayMs(Infinity, 'seconds')).toThrow('Wait amount');
  expect(() => getWaitDelayMs(1, 'weeks')).toThrow('Wait unit');
});

test.each([
  [{ resume: 'webhook' }, 'After Time Interval'],
  [{ amount: -1 }, 'Wait amount'],
  [{ unit: 'weeks' }, 'Wait unit'],
] as const)('strict mode reports unsupported Wait parameters %j', (parameters, expected) => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-wait-invalid-'));
  try {
    const { result } = compileWait(dir, 1.1, parameters);
    expect(result.exitCode).toBe(1);
    expect(result.stderr.toString()).toContain(expected);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
