import { expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

function compileJsonFixture(directory: string, parameters: Record<string, unknown>) {
  const input = path.join(directory, 'records.json');
  const output = path.join(directory, 'service');
  writeFileSync(input, JSON.stringify({
    nodes: [{
      name: 'Make file',
      type: 'n8n-nodes-base.convertToFile',
      typeVersion: 1.1,
      parameters: { operation: 'toJson', ...parameters },
    }],
  }));
  const result = Bun.spawnSync([
    process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', output, '--strict',
  ]);
  return { output, result };
}

test('generated Convert to File creates one JSON binary from two input items', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-json-file-'));
  try {
    const { output, result } = compileJsonFixture(dir, {
      binaryPropertyName: '={{ $json.field }}',
      options: {
        encoding: 'utf8',
        fileName: '=records-{{ $json.id }}.json',
        format: true,
        addBOM: true,
      },
    });
    expect(result.exitCode).toBe(0);
    const manifest = JSON.parse(readFileSync(path.join(output, 'package.json'), 'utf8'));
    expect(manifest.dependencies['mime-types']).toBeDefined();
    expect(manifest.dependencies['@aws-sdk/client-s3']).toBeUndefined();

    const moduleDir = path.join(output, 'node_modules', 'mime-types');
    mkdirSync(moduleDir, { recursive: true });
    writeFileSync(path.join(moduleDir, 'package.json'), '{"main":"index.cjs"}');
    writeFileSync(path.join(moduleDir, 'index.cjs'), 'exports.extension = () => "json";');

    const { workflow_records } = await import(path.join(output, 'workflows', 'records.ts'));
    const input = [
      { json: { id: 'first', field: 'attachment', label: 'été' }, binary: { ignored: { data: 'AA==' } } },
      { json: { id: 'second', field: 'different', label: 'hiver' } },
    ];
    const { items } = await workflow_records(input);
    expect(items).toHaveLength(1);
    expect(items[0].json).toEqual({});
    expect(items[0].pairedItem).toEqual([{ item: 0 }, { item: 1 }]);
    expect(Object.keys(items[0].binary)).toEqual(['attachment']);
    const file = items[0].binary.attachment;
    expect(file).toMatchObject({
      fileName: 'records-first.json', mimeType: 'application/json', fileExtension: 'json', fileType: 'json',
    });
    const bytes = Buffer.from(file.data, 'base64');
    expect(bytes.subarray(0, 3)).toEqual(Buffer.from([0xef, 0xbb, 0xbf]));
    expect(bytes.subarray(3).toString('utf8')).toBe(JSON.stringify(input.map(item => item.json), null, 2));
    expect((await workflow_records([])).items).toEqual([]);

    const { convertItemsToJsonFile } = await import(path.join(output, 'runtime', 'convert-to-json.ts'));
    const defaults = convertItemsToJsonFile(input, {
      binaryProperty: 'data', fileName: '', format: false, addBOM: false,
    }).binary.data;
    expect(defaults.fileName).toBe('file.json');
    expect(Buffer.from(defaults.data, 'base64').toString('utf8')).toBe(JSON.stringify(input.map(item => item.json)));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test.each([
  [{ operation: 'toCsv' }, 'supports Convert to JSON'],
  [{ mode: 'each' }, 'supports the All Items to One File mode'],
  [{ options: { encoding: 'ucs2' } }, 'supports UTF-8 encoding'],
] as const)('strict mode rejects unsupported Convert to File parameters %j', (parameters, message) => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-json-file-strict-'));
  try {
    const { result } = compileJsonFixture(dir, parameters);
    expect(result.exitCode).toBe(1);
    expect(result.stderr.toString()).toContain(message);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
