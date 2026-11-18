import { expect, test } from 'bun:test';
import { copyFileSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Writable } from 'node:stream';
import Fastify from 'fastify';
import { WorkflowParser } from '../src/compiler/parser';
import { CodeGenerator } from '../src/compiler/code-generator';
import { RouteGenerator } from '../src/compiler/route-generator';

test('generated webhook exceptions return a correlation ID without exposing exception details', async () => {
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

    const workflows = WorkflowParser.parseInput(input);
    mkdirSync(path.join(service, 'workflows'), { recursive: true });
    mkdirSync(path.join(service, 'runtime'), { recursive: true });
    mkdirSync(path.join(service, 'routes'), { recursive: true });
    for (const file of readdirSync(path.resolve('src/runtime'))) {
      if (file.endsWith('.ts')) {
        copyFileSync(path.resolve('src/runtime', file), path.join(service, 'runtime', file));
      }
    }
    for (const wf of workflows) {
      writeFileSync(path.join(service, 'workflows', wf.relativePath), CodeGenerator.generateWorkflowCode(wf), 'utf8');
    }
    writeFileSync(path.join(service, 'routes', 'index.ts'), RouteGenerator.generateRoutesCode(workflows), 'utf8');

    const chunks: string[] = [];
    const stream = new Writable({
      write(chunk, _encoding, done) { chunks.push(chunk.toString()); done(); },
    });
    const { registerCompiledRoutes } = await import(path.join(service, 'routes', 'index.ts'));
    const server = Fastify({ logger: { stream } });
    await server.register(registerCompiledRoutes);
    const address = await server.listen({ port: 0, host: '127.0.0.1' });
    let status: number;
    let body: any;
    try {
      const response = await fetch(`${address}/webhook/fixture`, {
        method: 'POST',
        headers: { 'request-id': 'client-supplied-id' },
      });
      status = response.status;
      body = await response.json();
    } finally {
      await server.close();
    }

    const rawLogs = chunks.join('');
    expect(JSON.stringify(body)).not.toContain(marker);
    expect(rawLogs).not.toContain(marker);
    expect(status).toBe(500);
    expect(body).toEqual({
      error: 'Workflow Execution Error',
      message: 'Workflow execution failed',
      errorId: expect.any(String),
    });
    const logs = rawLogs.trim().split('\n').map(line => JSON.parse(line));
    const failure = logs.find((entry: { level: number }) => entry.level === 50);
    expect(failure).toMatchObject({
      errorId: body.errorId,
      msg: 'Workflow execution failed',
    });
    expect(failure.err).toBeUndefined();
    expect(body.errorId).not.toBe('client-supplied-id');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
