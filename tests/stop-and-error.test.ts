import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

test.each([
  [{ errorMessage: '={{ $json.reason }}' }, undefined],
  [{ errorType: 'errorObject', errorObject: '={"description":"fixture failure","code":42}' },
    { description: 'fixture failure', code: 42 }],
] as const)('compiled Stop and Error only fails an active branch %j', async (parameters, metadata) => {
  const dir = mkdtempSync(path.join(tmpdir(), 'n2f-terminal-'));
  try {
    const json = path.join(dir, 'workflow.json');
    const output = path.join(dir, 'generated');
    writeFileSync(json, JSON.stringify({
      nodes: [
        {
          name: 'Decide', type: 'n8n-nodes-base.if', typeVersion: 2.2,
          parameters: { conditions: { conditions: [
            { leftValue: '={{ $json.fail }}', operator: { operation: 'true' } },
          ] } },
        },
        { name: 'Stop', type: 'n8n-nodes-base.stopAndError', typeVersion: 1, parameters },
        { name: 'Pass', type: 'n8n-nodes-base.code', parameters: { jsCode: 'return $input.all();' } },
      ],
      connections: { Decide: { main: [
        [{ node: 'Stop', type: 'main', index: 0 }],
        [{ node: 'Pass', type: 'main', index: 0 }],
      ] } },
    }));
    const result = Bun.spawnSync([
      process.execPath, path.resolve('bin/cli.ts'), 'compile', json, '--out', output, '--strict',
    ]);
    expect(result.exitCode).toBe(0);
    const { workflow_workflow } = await import(path.join(output, 'workflows', 'workflow.ts'));
    const reason = 'fixture failure';
    const success = [{ json: { fail: false, reason } }];
    const { context, items } = await workflow_workflow(success);
    expect(items).toEqual(success);
    expect(context.getNode('Pass').all()).toEqual(success);
    await expect(workflow_workflow([{ json: { fail: true, reason } }])).rejects.toMatchObject({
      name: 'WorkflowStopError', message: reason, metadata,
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
