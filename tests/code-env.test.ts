import { expect, test } from 'bun:test';
import fs from 'fs';
import path from 'path';
import os from 'os';

test('generated Code nodes and expressions read the runtime .env', () => {
  const serviceDir = fs.mkdtempSync(path.join(os.tmpdir(), 'n2f-runtime-env-'));
  try {
    const input = path.join(serviceDir, 'config.json');
    fs.writeFileSync(input, JSON.stringify({
      nodes: [{
        name: 'Read environment',
        type: 'n8n-nodes-base.code',
        parameters: {
          jsCode: 'return [{ json: { dot: $env.N2F_TEST_VALUE, bracket: $env["N2F_TEST_VALUE"] } }];',
        },
      }, {
        name: 'Read with expressions',
        type: 'n8n-nodes-base.set',
        typeVersion: 3.4,
        parameters: {
          includeOtherFields: true,
          assignments: { assignments: [
            { name: 'expressionDot', value: '={{ $env.N2F_TEST_VALUE }}' },
            { name: 'expressionBracket', value: '={{ $env["N2F_TEST_VALUE"] }}' },
          ] },
        },
      }],
      connections: {
        'Read environment': { main: [[{ node: 'Read with expressions', type: 'main', index: 0 }]] },
      },
    }));
    const compile = Bun.spawnSync(
      [process.execPath, path.resolve('bin/cli.ts'), 'compile', input, '--out', serviceDir]
    );
    expect(compile.exitCode).toBe(0);

    fs.writeFileSync(path.join(serviceDir, '.env'), 'N2F_TEST_VALUE=runtime-value\n');
    fs.writeFileSync(path.join(serviceDir, 'verify.ts'), [
      'import { workflow_config } from "./workflows/config";',
      'console.log(JSON.stringify((await workflow_config()).items));',
    ].join('\n'));

    const executed = Bun.spawnSync([process.execPath, 'run', 'verify.ts'], { cwd: serviceDir });
    expect(executed.exitCode).toBe(0);
    expect(JSON.parse(executed.stdout.toString())).toEqual([{
      json: {
        dot: 'runtime-value',
        bracket: 'runtime-value',
        expressionDot: 'runtime-value',
        expressionBracket: 'runtime-value',
      },
    }]);
  } finally {
    fs.rmSync(serviceDir, { recursive: true, force: true });
  }
});
