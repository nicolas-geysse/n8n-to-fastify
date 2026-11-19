import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const fixture = (executionOrder: string) => {
  const node = (name: string, type: string, position: [number, number], parameters = {}, typeVersion = 2) => ({
    name, type: `n8n-nodes-base.${type}`, position, parameters, typeVersion,
  });
  const code = (name: string, position: [number, number], jsCode: string) => node(name, 'code', position, { jsCode });
  const request = (stage: string, label: string, position: [number, number]) => code(stage, position, `
return $input.all().map(({ json }) => {
  json.events.push(${JSON.stringify(stage)} + ':' + json.label + ':' + json.attempt);
  return { json: { ...json, ready: json.label === ${JSON.stringify(label)} && json.attempt === 1 } };
});`);
  const ready = (name: string, position: [number, number]) => node(name, 'if', position, { conditions: { conditions: [
    { leftValue: '={{ $json.ready }}', operator: { type: 'boolean', operation: 'true' } },
  ] } }, 2.2);
  const flow: Record<string, string[]> = {
    Seed: ['Primary'], Primary: ['Primary Ready'], 'Primary Ready': ['Report', 'Fallback'],
    Fallback: ['Fallback Ready'], 'Fallback Ready': ['Report', 'Decide'], Decide: ['Route'],
    Route: ['Pause', 'Reset', 'Exhausted'], Pause: ['Primary'], Reset: ['Primary'],
    Exhausted: ['Report'], Report: ['Collect'],
  };
  return {
    settings: { executionOrder },
    nodes: [
      code('Seed', [0, 0], 'return $input.all();'),
      request('Primary', 'wait', [100, 0]),
      ready('Primary Ready', [200, 0]),
      request('Fallback', 'reset', [300, 100]),
      ready('Fallback Ready', [400, 100]),
      code('Decide', [500, 200], `return $input.all().map(({ json }) => ({ json: {
        ...json, attempt: json.attempt + 1, port: json.attempt >= 2 ? 2 : json.label === 'reset' ? 1 : 0,
      } }));`),
      node('Route', 'switch', [600, 200], { mode: 'expression', numberOutputs: 3, output: '={{ $json.port }}' }, 3.3),
      node('Pause', 'wait', [700, 100], { resume: 'timeInterval', amount: 0, unit: 'seconds' }, 1.1),
      code('Reset', [700, 200], 'return $input.all();'),
      code('Exhausted', [700, 300], 'return $input.all();'),
      code('Report', [800, -100], `return $input.all().map(({ json }) => {
        json.events.push('Report:' + json.label + ':' + json.attempt);
        return { json: { label: json.label, attempt: json.attempt } };
      });`),
      node('Collect', 'aggregate', [900, -100], { destinationFieldName: 'batch' }),
    ],
    connections: Object.fromEntries(Object.entries(flow).map(([source, targets]) => [source, {
      main: targets.map(target => [{ node: target, type: 'main', index: 0 }]),
    }])),
  };
};

test('v1 retries dispatch depth-first and keep completed batches separate at the boundary', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-polling-returns-'));
  const previousLimit = process.env.N8N_TO_FASTIFY_POLL_MAX_ATTEMPTS;
  try {
    const source = path.join(dir, 'returns.json');
    const output = path.join(dir, 'service');
    const compile = (executionOrder: string) => {
      writeFileSync(source, JSON.stringify(fixture(executionOrder)));
      return Bun.spawnSync([process.execPath, path.resolve('bin/cli.ts'), 'compile', source, '--out', output, '--strict']);
    };
    const legacy = compile('v0');
    expect(legacy.exitCode).toBe(1);
    expect(legacy.stderr.toString()).toContain('Cyclic dependency detected');
    const modern = compile('v1');
    expect(modern.stderr.toString()).toBe('');
    expect(modern.exitCode).toBe(0);
    const { workflow_returns } = await import(path.join(output, 'workflows', 'returns.ts'));
    const events: string[] = [];
    const input = (...labels: string[]) => labels.map(label => ({ json: { label, attempt: 0, events } }));
    process.env.N8N_TO_FASTIFY_POLL_MAX_ATTEMPTS = '10';
    const run = await workflow_returns(input('wait', 'reset', 'exhausted'));
    expect(events).toEqual([
      'Primary:wait:0', 'Primary:reset:0', 'Primary:exhausted:0',
      'Fallback:wait:0', 'Fallback:reset:0', 'Fallback:exhausted:0',
      'Primary:wait:1', 'Primary:exhausted:1', 'Report:wait:1', 'Fallback:exhausted:1',
      'Primary:exhausted:2', 'Fallback:exhausted:2', 'Report:exhausted:3',
      'Primary:reset:1', 'Fallback:reset:1', 'Report:reset:1',
    ]);
    expect(run.items).toEqual([
      { json: { batch: [{ label: 'wait', attempt: 1 }] } },
      { json: { batch: [{ label: 'exhausted', attempt: 3 }] } },
      { json: { batch: [{ label: 'reset', attempt: 1 }] } },
    ]);

    events.length = 0;
    process.env.N8N_TO_FASTIFY_POLL_MAX_ATTEMPTS = '2';
    await expect(workflow_returns(input('wait', 'reset'))).rejects.toThrow('Polling loop exceeded its limit of 2 attempts');
    expect(events.at(-1)).toBe('Report:wait:1');
  } finally {
    if (previousLimit === undefined) delete process.env.N8N_TO_FASTIFY_POLL_MAX_ATTEMPTS;
    else process.env.N8N_TO_FASTIFY_POLL_MAX_ATTEMPTS = previousLimit;
    rmSync(dir, { recursive: true, force: true });
  }
});
