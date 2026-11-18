import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import type { CompiledWorkflowFn } from '../src/runtime/workflow-registry';

async function withEditFields(
  parameters: Record<string, unknown>,
  check: (compile: ReturnType<typeof Bun.spawnSync>, load: () => Promise<CompiledWorkflowFn>) => Promise<void>,
) {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-json-output-'));
  try {
    const input = path.join(dir, 'records.json');
    const output = path.join(dir, 'service');
    writeFileSync(input, JSON.stringify({
      nodes: [
        { name: 'Source', type: 'n8n-nodes-base.code', parameters: { jsCode: 'return $input.all();' } },
        {
          name: 'Edit', type: 'n8n-nodes-base.set', typeVersion: 3.4,
          parameters: { mode: 'raw', ...parameters },
        },
      ],
      connections: { Source: { main: [[{ node: 'Edit', type: 'main', index: 0 }]] } },
    }));
    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', output, '--strict',
    ]);
    await check(compile, async () => (await import(path.join(output, 'workflows', 'records.ts'))).workflow_records);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('JSON Output renders typed and quoted values at each legacy upstream item index', async () => {
  await withEditFields({
    jsonOutput: '={ "text": "{{ $node["Source"].json.name }}", "values": {{ $node["Source"].json.values }}, "stats.count": {{ $input.all().length }} }',
  }, async (compile, load) => {
    expect(compile.exitCode).toBe(0);
    const run = await load();
    const input = [
      { json: { name: 'a "quote"\nand a newline', values: [1, false] }, binary: { data: { data: 'AA==' } } },
      { json: { name: 'two', values: { n: 3 } } },
    ];
    const { items } = await run(input);
    expect(items).toEqual([
      { json: { text: input[0].json.name, values: [1, false], stats: { count: 2 } }, pairedItem: { item: 0 } },
      { json: { text: 'two', values: { n: 3 }, stats: { count: 2 } }, pairedItem: { item: 1 } },
    ]);
  });
});

test('JSON Output can merge fields and preserve binary data without mutating input', async () => {
  await withEditFields({
    jsonOutput: '{"meta.updated": true}',
    includeOtherFields: true,
    include: 'all',
    options: { stripBinary: false },
  }, async (compile, load) => {
    expect(compile.exitCode).toBe(0);
    const run = await load();
    const input = { json: { meta: { label: 'kept' } }, binary: { fixture: { data: 'AA==' } } };
    expect((await run([input])).items).toEqual([
      { json: { meta: { label: 'kept', updated: true } }, binary: input.binary, pairedItem: { item: 0 } },
    ]);
    expect(input.json.meta).toEqual({ label: 'kept' });
  });
});

test('JSON Output accepts an object-valued expression and reports invalid JSON safely', async () => {
  await withEditFields({ jsonOutput: '={{ $json.value }}' }, async (compile, load) => {
    expect(compile.exitCode).toBe(0);
    const run = await load();
    expect((await run([{ json: { value: { label: 'one' } } }])).items).toEqual([
      { json: { label: 'one' }, pairedItem: { item: 0 } },
    ]);
    await expect(run([{ json: { value: '[1]' } }])).rejects.toThrow('expects an object');
    await expect(run([{ json: { value: '{invalid fixture' } }])).rejects.toThrow('contains invalid JSON at item 0');
  });
});

test('strict mode reports field selection modes outside JSON Output support', async () => {
  await withEditFields({
    jsonOutput: '{}', includeOtherFields: true, include: 'selected', includeFields: 'one',
  }, async (compile) => {
    expect(compile.exitCode).toBe(1);
    expect(compile.stderr.toString()).toContain('supports including all or none of the input fields');
  });
});
