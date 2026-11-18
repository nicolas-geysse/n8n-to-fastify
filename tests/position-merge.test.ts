import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { mergeByPosition } from '../src/runtime/position-merge';

const left = [
  { json: { left: 1, shared: 'left', nested: { left: 1 } }, binary: { sample: { fileName: 'example.txt' } } },
  { json: { left: 2 } },
];
const middle = [{ json: { middle: 1, shared: 'middle', nested: { middle: 1 } } }];
const right = [
  { json: { right: 1, shared: 'right', nested: { right: 1 } }, binary: { sample: { mimeType: 'text/plain' } } },
  { json: { right: 2 } },
  { json: { right: 3 } },
];

test.each([[false], [true]])('position Merge uses destination ports with includeUnpaired=%s', async includeUnpaired => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-position-merge-'));
  try {
    const input = path.join(dir, 'merge.json');
    const output = path.join(dir, 'service');
    const nodes = Object.entries({ Left: left, Middle: middle, Right: right }).map(([name, items]) => ({
      name,
      type: 'n8n-nodes-base.code',
      parameters: { jsCode: `return ${JSON.stringify(items)};` },
    }));
    const target = (index: number) => ({ main: [[{ node: 'Merge', type: 'main', index }]] });
    writeFileSync(input, JSON.stringify({
      nodes: [...nodes, {
        name: 'Merge',
        type: 'n8n-nodes-base.merge',
        typeVersion: 3,
        parameters: {
          mode: 'combine', combineBy: 'combineByPosition', numberInputs: 3,
          options: { includeUnpaired },
        },
      }],
      connections: { Right: target(2), Left: target(0), Middle: target(1) },
    }));
    const compiled = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', output, '--strict',
    ]);
    expect(compiled.exitCode).toBe(0);
    const { workflow_merge } = await import(path.join(output, 'workflows', 'merge.ts'));
    const result = await workflow_merge();
    const paired = {
      json: {
        left: 1, middle: 1, right: 1, shared: 'right',
        nested: { left: 1, middle: 1, right: 1 },
      },
      binary: { sample: { fileName: 'example.txt', mimeType: 'text/plain' } },
    };
    expect(result.items).toEqual(includeUnpaired
      ? [paired, { json: { left: 2, right: 2 } }, { json: { right: 3 } }]
      : [paired]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('position Merge preserves two-input clash options and handles empty inputs', () => {
  expect(mergeByPosition([left, right], false, { mergeMode: 'shallowMerge', resolveClash: 'preferInput1' })[0].json)
    .toEqual({ left: 1, right: 1, shared: 'left', nested: { left: 1 } });
  expect(mergeByPosition([left.slice(0, 1), [{ json: { shared: '' } }]], false, { overrideEmpty: true })[0].json.shared)
    .toBe('left');
  expect(mergeByPosition([left, right], false, { resolveClash: 'addSuffix' })[0].json)
    .toMatchObject({ shared_1: 'left', shared_2: 'right' });
  expect(mergeByPosition([left, [], right], false, {})).toEqual([]);
});
