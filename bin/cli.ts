#!/usr/bin/env bun
import { Command } from 'commander';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { WorkflowParser } from '../src/compiler/parser';
import { CodeGenerator } from '../src/compiler/code-generator';
import { getCompatibilityIssues } from '../src/compiler/compatibility';
import { RouteGenerator } from '../src/compiler/route-generator';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const program = new Command();

program
  .name('n8n-to-fastify')
  .description('Ahead-Of-Time (AOT) compiler that transpiles n8n workflows into bare-metal Fastify microservices')
  .version('0.1.0');

const compile = program
  .command('compile')
  .option('--strict', 'Reject known unsupported workflow features before generating output')
  .description('Compile a single workflow.json or a directory of workflows into a standalone Fastify project')
  .argument('<input>', 'Path to a workflow .json file or directory of workflows')
  .option('-o, --out <dir>', 'Output directory for the generated microservice', './compiled-service')
  .action((input: string, options: { out: string }) => {
    const startTime = performance.now();
    const outDir = path.resolve(options.out);

    console.log(`\n⚡ [n8n-to-fastify] Parsing workflows from: ${path.resolve(input)}`);

    const { strict } = compile.opts<{ strict?: boolean }>();
    const workflows = WorkflowParser.parseInput(input, { failOnParseError: strict });
    if (workflows.length === 0) {
      console.error('❌ No valid n8n workflows found.');
      process.exit(1);
    }

    if (strict) {
      const issues = workflows.flatMap(getCompatibilityIssues);
      if (issues.length > 0) {
        program.error(`[n8n-to-fastify] Strict compatibility check failed.\n${issues.map(issue => `  - ${issue}`).join('\n')}`);
      }
    }

    // Prepare output directories
    const workflowsOutDir = path.join(outDir, 'workflows');
    const routesOutDir = path.join(outDir, 'routes');
    const runtimeOutDir = path.join(outDir, 'runtime');

    fs.mkdirSync(workflowsOutDir, { recursive: true });
    fs.mkdirSync(routesOutDir, { recursive: true });
    fs.mkdirSync(runtimeOutDir, { recursive: true });

    // Copy runtime files
    const srcRuntimeDir = path.resolve(__dirname, '../src/runtime');
    for (const file of fs.readdirSync(srcRuntimeDir)) {
      if (file.endsWith('.ts')) {
        fs.copyFileSync(path.join(srcRuntimeDir, file), path.join(runtimeOutDir, file));
      }
    }

    // Compile each workflow
    for (const wf of workflows) {
      const targetPath = path.join(workflowsOutDir, wf.relativePath);
      fs.mkdirSync(path.dirname(targetPath), { recursive: true });
      const code = CodeGenerator.generateWorkflowCode(wf);
      fs.writeFileSync(targetPath, code, 'utf8');
      console.log(`  ✓ Compiled: ${wf.name} -> workflows/${wf.relativePath}`);
    }

    // Generate Fastify routes, server entry, and package.json
    fs.writeFileSync(
      path.join(routesOutDir, 'index.ts'),
      RouteGenerator.generateRoutesCode(workflows),
      'utf8'
    );
    fs.writeFileSync(path.join(outDir, 'server.ts'), RouteGenerator.generateServerEntry(), 'utf8');
    fs.writeFileSync(
      path.join(outDir, 'package.json'),
      RouteGenerator.generateOutputPackageJson(),
      'utf8'
    );

    const elapsedMs = (performance.now() - startTime).toFixed(2);
    console.log(`\n🚀 Compilation finished in ${elapsedMs}ms!`);
    console.log(`📁 Output ready at: ${outDir}\n`);
    console.log(`Next steps:`);
    console.log(`  cd ${options.out}`);
    console.log(`  bun install`);
    console.log(`  bun run start\n`);
  });

program.parse();
