import { expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test('generated HTTP Request sends multipart bytes and JSON headers over loopback', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-http-form-'));
  let requestCount = 0;
  const receiver = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      requestCount++;
      const contentType = request.headers.get('content-type');
      const marker = request.headers.get('x-fixture');
      if (new URL(request.url).pathname === '/json') {
        return Response.json({ contentType, marker, body: await request.json() });
      }
      const form = await request.formData();
      const file = form.get('attachment');
      if (!(file instanceof File)) return new Response('missing file', { status: 400 });
      return Response.json({
        contentType,
        marker,
        labels: form.getAll('label'),
        fileName: file.name,
        mimeType: file.type,
        bytes: Array.from(new Uint8Array(await file.arrayBuffer())),
      });
    },
  });

  try {
    const inputs = path.join(dir, 'workflows');
    const service = path.join(dir, 'service');
    mkdirSync(inputs);
    const url = `http://127.0.0.1:${receiver.port}`;
    const write = (name: string, parameters: Record<string, unknown>) => writeFileSync(
      path.join(inputs, `${name}.json`),
      JSON.stringify({ nodes: [{
        name, type: 'n8n-nodes-base.httpRequest', typeVersion: 4.2, parameters,
      }] }),
    );
    write('multipart', {
      method: 'POST', url, sendHeaders: true, specifyHeaders: 'json',
      jsonHeaders: '={{ JSON.stringify({ "X-Fixture": $json.marker, "Content-Type": "multipart/form-data", "Content-Length": "1" }) }}',
      sendBody: true, contentType: 'multipart-form-data',
      bodyParameters: { parameters: [
        { parameterType: 'formBinaryData', name: 'attachment', inputDataFieldName: '={{ $json.property }}' },
        { name: 'label', value: '={{ $json.label }}' },
        { name: 'label', value: 'static' },
      ] },
    });
    write('json', {
      method: 'POST', url: `${url}/json`,
      headerParameters: { parameters: [{ name: 'X-Fixture', value: 'legacy' }] },
      jsonBody: '={{ ({ label: $json.label }) }}',
    });

    const compile = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', inputs, '--out', service, '--strict',
    ]);
    expect(compile.exitCode).toBe(0);
    const workflows = path.join(service, 'workflows');
    const { workflow_multipart } = await import(path.join(workflows, 'multipart.ts'));
    const { workflow_json } = await import(path.join(workflows, 'json.ts'));
    const json = { marker: 'per-item', label: 'Résumé', property: 'downloaded' };
    const input = { json, binary: {
      downloaded: { data: 'AP+ADQo=', fileName: 'fixture.bin', mimeType: 'application/octet-stream' },
    } };
    const { items } = await workflow_multipart([input]);
    expect(items[0].json).toEqual({
      contentType: expect.stringMatching(/^multipart\/form-data;\s*boundary=.+/i),
      marker: 'per-item',
      labels: ['Résumé', 'static'],
      fileName: 'fixture.bin',
      mimeType: 'application/octet-stream',
      bytes: [0, 255, 128, 13, 10],
    });
    await expect(workflow_multipart([{ json }])).rejects.toThrow('missing the configured binary field');
    await expect(workflow_multipart([{ json, binary: {
      downloaded: { id: 'fixture-storage-id', data: 'filesystem-v2' },
    } }])).rejects.toThrow('requires inline binary data');
    expect(requestCount).toBe(1);

    const response = await workflow_json([{ json: { label: 'ordinary' } }]);
    expect(response.items[0].json).toEqual({
      contentType: 'application/json', marker: 'legacy', body: { label: 'ordinary' },
    });
  } finally {
    await receiver.stop(true);
    rmSync(dir, { recursive: true, force: true });
  }
});
