import { expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test.each([
  { property: undefined, field: 'data', endpoint: '/fixture.bin', fileName: 'fixture.bin', fullResponse: false },
  { property: '={{ $json.field }}', field: 'attachment', endpoint: '/download', fileName: 'attachment.bin', fullResponse: false },
  { property: 'attachment', field: 'attachment', endpoint: '/download', fileName: 'attachment.bin', fullResponse: true },
  { property: 'attachment', field: 'attachment', endpoint: '/download', fileName: 'attachment.bin', fullResponse: '={{ $json.withMetadata }}' },
  { property: 'attachment', field: 'attachment', endpoint: '/report.custom?download=1', fileName: 'report.custom', fullResponse: true, extension: 'custom', mime: 'application/x-n2f-fixture-custom' },
])('compiled HTTP file responses preserve bytes and emit $field with fullResponse=$fullResponse', async ({ property, field, endpoint, fileName, fullResponse, extension = 'bin', mime = 'application/octet-stream' }) => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-http-file-'));
  const bytes = Uint8Array.of(0, 80, 255);
  const receiver = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    fetch: () => new Response(bytes, {
      status: 206,
      statusText: 'Partial Content',
      headers: [
        ['Content-Type', mime],
        ['X-Fixture', 'metadata'],
        ['Set-Cookie', 'first=1; Path=/'],
        ['Set-Cookie', 'second=2; Path=/'],
      ],
    }),
  });

  try {
    const file = path.join(dir, 'download.json');
    const service = path.join(dir, 'service');
    writeFileSync(file, JSON.stringify({
      nodes: [{
        name: 'Download', type: 'n8n-nodes-base.httpRequest', typeVersion: 4.2,
        parameters: {
          url: new URL(endpoint, receiver.url).href,
          options: { response: { response: { responseFormat: 'file', outputPropertyName: property, fullResponse } } },
        },
      }],
    }));

    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', file, '--out', service, '--strict',
    ]);
    expect(compile.exitCode).toBe(0);
    symlinkSync(path.resolve('node_modules'), path.join(service, 'node_modules'), 'junction');
    const { workflow_download } = await import(path.join(service, 'workflows', 'download.ts'));
    const original = { data: 'AA==', mimeType: 'application/octet-stream' };
    const inputs = [
      { json: { field: 'attachment', withMetadata: true }, binary: { original, [field]: original } },
      { json: { field: 'different' } },
    ];
    const { items } = await workflow_download(inputs);

    expect(items).toHaveLength(2);
    for (const [index, item] of items.entries()) {
      expect(item.binary?.[field]).toMatchObject({
        data: Buffer.from(bytes).toString('base64'),
        fileName,
        fileExtension: extension,
        mimeType: mime,
      });
      if (fullResponse) {
        expect(Object.keys(item.json).sort()).toEqual(['headers', 'statusCode', 'statusMessage']);
        expect(item.json).toMatchObject({
          statusCode: 206,
          statusMessage: 'Partial Content',
          headers: {
            'content-type': mime,
            'x-fixture': 'metadata',
            'set-cookie': ['first=1; Path=/', 'second=2; Path=/'],
          },
        });
      } else {
        expect(item.json).toEqual(inputs[index].json);
      }
      expect(item.pairedItem).toEqual({ item: index });
    }
    expect(items[0].binary.original).toEqual(original);
    expect(inputs[0].binary?.[field]).toEqual(original);
    const manifest = JSON.parse(readFileSync(path.join(service, 'package.json'), 'utf8'));
    expect(manifest.dependencies['mime-types']).toBeDefined();
  } finally {
    await receiver.stop(true);
    rmSync(dir, { recursive: true, force: true });
  }
});
