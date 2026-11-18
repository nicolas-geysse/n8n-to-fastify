import { expect, test } from 'bun:test';
import fs from 'fs';
import path from 'path';
import os from 'os';

test('loads a declared npm package selected at runtime in a Code node', () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'n2f-dependencies-'));
  const serviceDir = path.join(tmpDir, 'service');
  const cli = path.resolve('bin/cli.ts');
  const commanderDir = path.resolve('node_modules/commander');

  try {
    fs.writeFileSync(path.join(tmpDir, 'config.json'), JSON.stringify({
      nodes: [{
        name: 'Read configuration',
        type: 'n8n-nodes-base.code',
        parameters: {
          jsCode: [
            'const { Command } = require($json.packageName);',
            'const { Command: ImportedCommand } = await import($json.packageName);',
            'return [{ json: { loaded: new Command() instanceof ImportedCommand } }];',
          ].join('\n'),
        },
      }],
    }));
    fs.writeFileSync(path.join(tmpDir, 'dependencies.json'), '{"commander":"12.1.0"}');

    const compile = Bun.spawnSync(
      [process.execPath, cli, 'compile', 'config.json', '--dependencies', 'dependencies.json', '--out', serviceDir],
      { cwd: tmpDir }
    );
    expect(compile.exitCode).toBe(0);
    const manifest = JSON.parse(fs.readFileSync(path.join(serviceDir, 'package.json'), 'utf8'));
    expect(manifest.dependencies).toEqual({
      '@fastify/cors': '^9.0.1',
      fastify: '^4.28.1',
      commander: '12.1.0',
    });

    fs.mkdirSync(path.join(serviceDir, 'node_modules'));
    fs.symlinkSync(commanderDir, path.join(serviceDir, 'node_modules', 'commander'), 'junction');
    fs.writeFileSync(path.join(serviceDir, 'verify.ts'), [
      'import { workflow_config } from "./workflows/config";',
      'console.log(JSON.stringify((await workflow_config([{ json: { packageName: "commander" } }])).items));',
    ].join('\n'));

    const executed = Bun.spawnSync([process.execPath, 'run', 'verify.ts'], { cwd: serviceDir });
    expect(executed.exitCode).toBe(0);
    expect(JSON.parse(executed.stdout.toString())).toEqual([{ json: { loaded: true } }]);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
