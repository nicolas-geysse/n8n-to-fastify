import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test('generated webhook exceptions return a correlation ID without exposing exception details', () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-webhook-error-'));
  try {
    const service = path.join(dir, 'service');
    const input = path.join(dir, 'fixture.json');
    const marker = 'fixture-only-sensitive-error';
    writeFileSync(input, JSON.stringify({
      nodes: [
        { name: 'Webhook', type: 'n8n-nodes-base.webhook', parameters: { path: 'fixture' } },
        {
          name: 'Fail',
          type: 'n8n-nodes-base.code',
          parameters: { jsCode: `throw new Error(${JSON.stringify(marker)});` },
        },
      ],
      connections: { Webhook: { main: [[{ node: 'Fail', type: 'main', index: 0 }]] } },
    }));
    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', service,
    ]);
    expect(compile.exitCode).toBe(0);

    writeFileSync(path.join(service, 'replay.ts'), `
      import { Writable } from 'node:stream';
      import Fastify from ${JSON.stringify(path.resolve('node_modules/fastify/fastify.js'))};
      import { registerCompiledRoutes } from './routes/index';
      const chunks: string[] = [];
      const stream = new Writable({
        write(chunk, encoding, done) { chunks.push(chunk.toString()); done(); },
      });
      const server = Fastify({ logger: { stream } });
      await server.register(registerCompiledRoutes);
      try {
        const response = await server.inject({
          method: 'POST', url: '/webhook/fixture', headers: { 'request-id': 'client-supplied-id' },
        });
        console.log(JSON.stringify({
          status: response.statusCode,
          body: response.json(),
          logs: chunks.join('').trim().split('\\n').map(line => JSON.parse(line)),
        }));
      } finally {
        await server.close();
      }
    `);

    const replay = Bun.spawnSync([process.execPath, 'run', 'replay.ts'], { cwd: service });
    expect(replay.exitCode).toBe(0);
    const output = replay.stdout.toString();
    expect(output).not.toContain(marker);
    expect(replay.stderr.toString()).not.toContain(marker);
    const result = JSON.parse(output);
    expect(result.status).toBe(500);
    expect(result.body).toEqual({
      error: 'Workflow Execution Error',
      message: 'Workflow execution failed',
      errorId: expect.any(String),
    });
    const failure = result.logs.find((entry: { level: number }) => entry.level === 50);
    expect(failure).toMatchObject({
      errorId: result.body.errorId,
      msg: 'Workflow execution failed',
    });
    expect(failure.err).toBeUndefined();
    expect(result.body.errorId).not.toBe('client-supplied-id');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
