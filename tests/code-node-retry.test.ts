import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test.each([
  ['stopWorkflow', true],
  ['continueRegularOutput', true],
  ['continueRegularOutput', false],
] as const)('Code retries the full batch with %s and recovery=%s', async (onError, recover) => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-code-retry-'));
  const attempts: unknown[] = [];
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      attempts.push(await request.json());
      return Response.json({ ok: recover && attempts.length > 1 });
    },
  });
  try {
    const input = path.join(dir, 'code-retry.json');
    const output = path.join(dir, 'service');
    writeFileSync(input, JSON.stringify({
      nodes: [{
        name: 'Code', type: 'n8n-nodes-base.code', typeVersion: 2,
        retryOnFail: true, maxTries: 2, waitBetweenTries: 1, onError,
        parameters: { jsCode: `
const response = await fetch(${JSON.stringify(server.url.href)}, {
  method: 'POST',
  body: JSON.stringify($input.all().map(row => row.json)),
});
if (!(await response.json()).ok) throw new Error('fixture failure');
return $input.all();
` },
      }],
      connections: {},
    }));
    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', output, '--strict',
    ]);
    expect(compile.stderr.toString()).toBe('');
    expect(compile.exitCode).toBe(0);
    const { workflow_code_retry } = await import(path.join(output, 'workflows', 'code-retry.ts'));
    const inputItems = [{ json: { value: 1 } }, { json: { value: 2 } }];
    const { context, items } = await workflow_code_retry(inputItems);
    const expected = recover ? inputItems : [{ json: { error: 'fixture failure' } }];
    expect(items).toEqual(expected);
    expect(context.getNodeRunItems('Code', 0, 0)).toEqual(expected);
    expect(() => context.getNodeRunItems('Code', 0, 1)).toThrow('Referenced node run does not exist');
    expect(attempts).toEqual(Array(2).fill(inputItems.map(item => item.json)));
  } finally {
    await server.stop(true);
    rmSync(dir, { recursive: true, force: true });
  }
});
