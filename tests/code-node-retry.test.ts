import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test('generated Code retries its entire input batch before publishing results', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-code-retry-'));
  const attempts: unknown[] = [];
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      attempts.push(await request.json());
      return Response.json({ ok: attempts.length > 1 });
    },
  });
  try {
    const input = path.join(dir, 'code-retry.json');
    const output = path.join(dir, 'service');
    writeFileSync(input, JSON.stringify({
      nodes: [{
        name: 'Code', type: 'n8n-nodes-base.code', typeVersion: 2,
        retryOnFail: true, maxTries: 2, waitBetweenTries: 1,
        parameters: { jsCode: `
const result = await this.helpers.httpRequest({
  url: ${JSON.stringify(server.url.href)},
  method: 'POST',
  body: $input.all().map(row => row.json),
  json: true,
});
if (!result.ok) throw new Error('fixture failure');
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
    expect((await workflow_code_retry(inputItems)).items).toEqual(inputItems);
    expect(attempts).toEqual(Array(2).fill(inputItems.map(item => item.json)));
  } finally {
    await server.stop(true);
    rmSync(dir, { recursive: true, force: true });
  }
});
