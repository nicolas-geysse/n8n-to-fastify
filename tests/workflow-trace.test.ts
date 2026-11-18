import { expect, test } from 'bun:test';
import fs from 'fs';
import os from 'os';
import path from 'path';
import type { NodeTraceEvent } from '../src/runtime/workflow-trace';
import { WorkflowContext } from '../src/runtime/workflow-context';

test('opt-in traces capture the generated webhook paths without exposing payloads or errors', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'n2f-trace-'));
  try {
    const input = path.join(dir, 'trace.json');
    const service = path.join(dir, 'service');
    const target = (node: string) => ({ node, type: 'main', index: 0 });
    fs.writeFileSync(input, JSON.stringify({
      id: 'trace-fixture',
      nodes: [
        { id: 'hook', name: 'Webhook', type: 'n8n-nodes-base.webhook', parameters: { path: 'trace' } },
        {
          id: 'decision', name: 'Decide', type: 'n8n-nodes-base.if',
          parameters: { conditions: { conditions: [
            { leftValue: '={{ $json.reject }}', rightValue: true, operator: { operation: 'equals' } },
          ] } },
        },
        {
          id: 'failure', name: 'Fail', type: 'n8n-nodes-base.code',
          parameters: { jsCode: 'throw new Error("fixture-error-detail");' },
        },
        {
          id: 'success', name: 'Accept', type: 'n8n-nodes-base.respondToWebhook',
          parameters: { respondWith: 'json', responseBody: '{"status":"accepted"}' },
        },
      ],
      connections: {
        Webhook: { main: [[target('Decide')]] },
        Decide: { main: [[target('Fail')], [target('Accept')]] },
      },
    }));
    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', service,
    ]);
    expect(compile.exitCode).toBe(0);
    fs.writeFileSync(path.join(service, 'replay.ts'), `
      import Fastify from ${JSON.stringify(path.resolve('node_modules/fastify/fastify.js'))};
      import { registerCompiledRoutes } from './routes/index';
      const fastify = Fastify();
      await fastify.register(registerCompiledRoutes);
      try {
        for (const reject of [false, true]) {
          const response = await fastify.inject({
            method: 'POST', url: '/webhook/trace', payload: { reject, value: 'fixture-input-detail' },
          });
          console.log(JSON.stringify({
            statusCode: response.statusCode,
            result: response.statusCode === 200 ? response.json() : undefined,
          }));
        }
      } finally {
        await fastify.close();
      }
    `);

    const replay = (trace: string) => {
      const run = Bun.spawnSync([process.execPath, 'run', 'replay.ts'], {
        cwd: service, env: { ...process.env, N8N_TO_FASTIFY_TRACE: trace },
      });
      expect(run.exitCode).toBe(0);
      const output = run.stdout.toString();
      expect(output).not.toContain('fixture-input-detail');
      expect(output).not.toContain('fixture-error-detail');
      const lines = output.trim().split('\n').map(line => JSON.parse(line));
      return {
        events: lines.filter(line => line.type === 'workflow.node') as NodeTraceEvent[],
        responses: lines.filter(line => 'statusCode' in line),
      };
    };

    const disabled = replay('0');
    expect(disabled.events).toEqual([]);
    const enabled = replay('1');
    expect(enabled.responses).toEqual([
      { statusCode: 200, result: { status: 'accepted' } },
      { statusCode: 500 },
    ]);
    expect(enabled.responses).toEqual(disabled.responses);
    expect(new Set(enabled.events.map(event => event.executionId)).size).toBe(2);
    expect(enabled.events.every(event =>
      event.schemaVersion === 1 && event.executionId.length > 0 && event.workflowId === 'trace-fixture'
    )).toBe(true);
    for (const event of enabled.events) {
      if ('durationMs' in event) expect(event.durationMs).toBeGreaterThanOrEqual(0);
    }

    const stable = enabled.events.map(event => [
      event.nodeId, event.status, ...('outputs' in event ? event.outputs.map(({ port, itemCount }) => [port, itemCount]) : []),
    ]);
    expect(stable).toEqual([
      ['hook', 'started'], ['hook', 'completed', [0, 1]],
      ['decision', 'started'], ['decision', 'completed', [1, 1]],
      ['success', 'started'], ['success', 'completed', [0, 1]],
      ['failure', 'skipped'],
      ['hook', 'started'], ['hook', 'completed', [0, 1]],
      ['decision', 'started'], ['decision', 'completed', [0, 1]],
      ['success', 'skipped'],
      ['failure', 'started'], ['failure', 'error'],
    ]);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('nested workflows share the execution ID and record their own outputs', () => {
  const events: NodeTraceEvent[] = [];
  const context = new WorkflowContext(event => { events.push(event); });
  const finishParent = context.startNodeTrace('parent', 'call', 'Shared name');
  const finishChild = context.startNodeTrace('child', 'step', 'Shared name');
  context.setNodeOutput('Shared name', [{ json: {} }, { json: {} }], 1);
  finishChild?.('completed');
  context.setNodeOutput('Shared name', [{ json: {} }], 0);
  finishParent?.('completed');

  expect(new Set(events.map(event => event.executionId)).size).toBe(1);
  expect(events.filter(event => event.status === 'completed').map(event => [
    event.workflowId, event.nodeId, event.outputs,
  ])).toEqual([
    ['child', 'step', [{ port: 1, itemCount: 2 }]],
    ['parent', 'call', [{ port: 0, itemCount: 1 }]],
  ]);
});

test('trace instrumentation preserves existing multiline Code literal values', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'n2f-trace-literals-'));
  try {
    const input = path.join(dir, 'literals.json');
    const service = path.join(dir, 'service');
    fs.writeFileSync(input, JSON.stringify({
      nodes: [
        { name: 'Webhook', type: 'n8n-nodes-base.webhook', parameters: { path: 'literals' } },
        ...['Standalone', 'Connected'].map(name => ({
          name,
          type: 'n8n-nodes-base.code',
          parameters: { jsCode: 'return [{ json: { value: `first\nsecond` } }];' },
        })),
      ],
      connections: { Webhook: { main: [[{ node: 'Connected', type: 'main', index: 0 }]] } },
    }));
    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', service,
    ]);
    expect(compile.exitCode).toBe(0);
    const { workflow_literals } = await import(path.join(service, 'workflows', 'literals.ts'));
    const { context } = await workflow_literals();
    expect(context.getNode('Standalone').first()?.json.value).toBe('first\n    second');
    expect(context.getNode('Connected').first()?.json.value).toBe('first\n      second');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
