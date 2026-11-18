import { expect, test } from 'bun:test';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createLegacyNodeReferences } from '../src/runtime/code-context';
import { WorkflowContext } from '../src/runtime/workflow-context';

test('compiled Code resolves dynamic $node references by the connected output port', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-legacy-node-'));
  try {
    const input = path.join(dir, 'fixture.json');
    const output = path.join(dir, 'service');
    const target = (name: string) => [{ node: name, type: 'main', index: 0 }];
    const fixture = {
      nodes: [
        { name: 'Source', type: 'n8n-nodes-base.code', typeVersion: 2,
          parameters: { jsCode: 'return $input.all();' } },
        { name: 'Decision', type: 'n8n-nodes-base.if', typeVersion: 2.2, parameters: {
          conditions: { conditions: [{
            leftValue: '={{ $json.ready }}', operator: { type: 'boolean', operation: 'true' },
          }] },
        } },
        { name: 'Read', type: 'n8n-nodes-base.code', typeVersion: 2, parameters: {
          mode: 'runOnceForAllItems',
          jsCode: [
            'const reference = ["Deci", "sion"].join("");',
            'const chosen = $node[reference];',
            'return [{ json: { selected: chosen.json.id, origin: $node["Source"].data.id,',
            '  metadata: chosen.binary, literal: \'$node["literal"]\' }, binary: $input.first().binary }];',
          ].join('\n'),
        } },
      ],
      connections: { Source: { main: [target('Decision')] }, Decision: { main: [[], target('Read')] } },
    };
    writeFileSync(input, JSON.stringify(fixture));
    const command = [process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--strict', '--out'];
    expect(Bun.spawnSync([...command, output]).exitCode).toBe(0);
    const { workflow_fixture } = await import(path.join(output, 'workflows', 'fixture.ts'));
    const binary = { data: { data: 'AAE=', fileName: 'fixture.bin', mimeType: 'application/octet-stream' } };
    const result = await workflow_fixture([
      { json: { ready: true, id: 'first' } },
      { json: { ready: false, id: 'second' }, binary },
    ]);
    expect(result.items).toEqual([{
      json: {
        selected: 'second', origin: 'first', literal: '$node["literal"]',
        metadata: { data: { fileName: 'fixture.bin', mimeType: 'application/octet-stream' } },
      },
      binary,
    }]);

    fixture.nodes[2].parameters.mode = 'runOnceForEachItem';
    writeFileSync(input, JSON.stringify(fixture));
    const rejected = path.join(dir, 'rejected');
    const resultEach = Bun.spawnSync([...command, rejected]);
    expect(resultEach.exitCode).toBe(1);
    expect(resultEach.stderr.toString()).toContain('legacy $node references require Run Once for All Items');
    expect(existsSync(rejected)).toBe(false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('legacy $node distinguishes unknown, unexecuted, disconnected and empty outputs', () => {
  const context = new WorkflowContext();
  context.setNodeOutput('Disconnected', [{ json: {} }]);
  context.setNodeOutput('Empty', []);
  const nodes = new Set(['Disconnected', 'Unexecuted', 'Empty']);
  const proxy = createLegacyNodeReferences(context, nodes, [['Unexecuted', 0], ['Empty', 0]]);
  expect(() => proxy.Missing.json).toThrow('Referenced node does not exist');
  expect(() => proxy.Unexecuted.json).toThrow('Referenced node has not executed');
  expect(() => proxy.Disconnected.json).toThrow('Referenced node is not connected');
  expect(() => proxy.Empty.json).toThrow('Referenced node output has no items');
});
