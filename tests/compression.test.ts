import { expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { gzipSync, strToU8, zipSync } from 'fflate';

function compileCompression(dir: string, parameters: Record<string, unknown> = {}) {
  const input = path.join(dir, 'archive.json');
  const service = path.join(dir, 'service');
  writeFileSync(input, JSON.stringify({
    nodes: [{ name: 'Unpack', type: 'n8n-nodes-base.compression', typeVersion: 1.1, parameters }],
  }));
  const result = Bun.spawnSync([
    process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', service, '--strict',
  ]);
  return { service, result };
}

test('compiled Compression unpacks ZIP and GZIP items with file metadata', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-decompress-'));
  try {
    const { service, result } = compileCompression(dir, { outputPrefix: 'entry_' });
    expect(result.exitCode).toBe(0);
    const manifest = JSON.parse(readFileSync(path.join(service, 'package.json'), 'utf8'));
    expect(manifest.dependencies.fflate).toBe('0.8.3');
    expect(manifest.dependencies['mime-types']).toBeDefined();
    expect(manifest.dependencies['@aws-sdk/client-s3']).toBeUndefined();
    mkdirSync(path.join(service, 'node_modules'));
    for (const name of ['fflate', 'mime-types']) {
      symlinkSync(path.resolve('node_modules', name), path.join(service, 'node_modules', name), 'dir');
    }
    const zip = zipSync({
      'sub/a.txt': strToU8('hello'), 'b.json': strToU8('{"value":1}'),
      '__MACOSX/ignored.txt': strToU8('ignored'),
    });
    const gz = gzipSync(strToU8('payload'));
    const input = [
      {
        json: { key: 'first' },
        binary: { data: { data: Buffer.from(zip).toString('base64'), fileName: 'input.zip', fileExtension: 'zip' } },
      },
      {
        json: { key: 'second' },
        binary: { data: { data: Buffer.from(gz).toString('base64'), fileName: 'input.txt.gz', fileExtension: 'GZ' } },
      },
    ];
    const { workflow_archive } = await import(path.join(service, 'workflows', 'archive.ts'));
    const { items } = await workflow_archive(input);
    expect(items.map((item: { json: unknown }) => item.json)).toEqual(input.map(item => item.json));
    expect(items.map((item: { pairedItem: unknown }) => item.pairedItem)).toEqual([{ item: 0 }, { item: 1 }]);
    expect(Object.keys(items[0].binary)).toEqual(['entry_0', 'entry_1']);
    expect(items[0].binary.entry_0).toMatchObject({ fileName: 'a.txt', directory: 'sub', mimeType: 'text/plain' });
    expect(items[0].binary.entry_1).toMatchObject({ fileName: 'b.json', mimeType: 'application/json' });
    expect(items[1].binary.entry_0).toMatchObject({ fileName: 'input.txt', fileExtension: 'txt' });
    expect([items[0].binary.entry_0, items[0].binary.entry_1, items[1].binary.entry_0]
      .map(file => Buffer.from(file.data, 'base64').toString('utf8'))).toEqual(['hello', '{"value":1}', 'payload']);
    await expect(workflow_archive([{ json: {} }])).rejects.toThrow('missing the configured binary field');
    await expect(workflow_archive([{ json: {}, binary: { data: { id: 'fixture-storage-ref' } } }]))
      .rejects.toThrow('requires inline base64 binary data');
    expect((await workflow_archive([])).items).toEqual([]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('strict mode reports unsupported Compression operations', () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-unsupported-compression-'));
  try {
    const { result } = compileCompression(dir, { operation: 'compress' });
    expect(result.exitCode).toBe(1);
    expect(result.stderr.toString()).toContain('supports ZIP and GZIP decompression');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
