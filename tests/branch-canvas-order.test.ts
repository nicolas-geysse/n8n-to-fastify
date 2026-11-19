import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test.each([
  ['v1', ['Upper', 'Upper Child', 'Left', 'Right']],
  ['v0', ['Right', 'Left', 'Upper', 'Upper Child']],
] as const)('generated branches use the canvas only when execution order is v1 (%s)', async (executionOrder, expected) => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-canvas-order-'));
  const code = (name: string, position: [number, number], record = true) => ({
    name, position, type: 'n8n-nodes-base.code', typeVersion: 2,
    parameters: {
      jsCode: `${record ? `$json.steps.push(${JSON.stringify(name)});` : ''} return $input.all();`,
    },
  });
  const outputs = (...names: string[]) => ({
    main: [names.map(node => ({ node, type: 'main', index: 0 }))],
  });
  try {
    const source = path.join(dir, 'order.json');
    const output = path.join(dir, 'service');
    writeFileSync(source, JSON.stringify({
      settings: { executionOrder },
      nodes: [
        code('Fork', [0, 0], false),
        code('Upper', [300, -100]),
        code('Upper Child', [400, 200]),
        code('Left', [100, 0]),
        code('Right', [200, 0]),
      ],
      connections: {
        Fork: outputs('Upper', 'Left', 'Right'),
        Upper: outputs('Upper Child'),
      },
    }));
    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', source, '--out', output, '--strict',
    ]);
    expect(compile.exitCode).toBe(0);
    const { workflow_order } = await import(path.join(output, 'workflows', 'order.ts'));
    const { items } = await workflow_order([{ json: { steps: [] } }]);
    expect(items[0].json.steps).toEqual(expected);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
