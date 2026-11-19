import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test('compiled manual Edit Fields follows the include settings for each input item', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-edit-include-'));
  try {
    const file = path.join(dir, 'edit.json');
    const compile = async (version: number, settings: Record<string, unknown>, output: string) => {
      writeFileSync(file, JSON.stringify({
        nodes: [{ name: 'Edit', type: 'n8n-nodes-base.set', typeVersion: version, parameters: {
          assignments: { assignments: [{ name: 'id', type: 'number', value: '={{ $json.id }}' }] }, ...settings,
        } }],
      }));
      const service = path.join(dir, output);
      const result = Bun.spawnSync([
        process.execPath, path.resolve('bin/cli.ts'), 'compile', file, '--out', service, '--strict',
      ]);
      expect(result.exitCode).toBe(0);
      return (await import(path.join(service, 'workflows', 'edit.ts'))).workflow_edit;
    };
    const first = { json: { id: 1, keep: true, label: 'one' } };
    const second = { json: { id: 2, keep: false, label: 'two' } };
    const defaultEdit = await compile(3.4, {}, 'default');
    expect((await defaultEdit([first])).items).toEqual([{ json: { id: 1 } }]);
    const dynamic = await compile(3.3, { includeOtherFields: '={{ $json.keep }}' }, 'dynamic');
    expect((await dynamic([first, second])).items).toEqual([first, { json: { id: 2 } }]);
    const none = await compile(3.4, { includeOtherFields: true, include: 'none' }, 'none');
    expect((await none([first])).items).toEqual([{ json: { id: 1 } }]);
    const older = await compile(3.2, {}, 'older');
    expect((await older([first])).items).toEqual([first]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
