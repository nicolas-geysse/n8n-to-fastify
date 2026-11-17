import { ParsedWorkflow } from './parser';
import { CodeGenerator } from './code-generator';

export class RouteGenerator {
  /**
   * Generates Fastify route definitions and a standalone server entry point
   * for all compiled workflows and webhook triggers.
   */
  static generateRoutesCode(workflows: ParsedWorkflow[]): string {
    let imports = `import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`;

    const importedFunctions: string[] = [];

    workflows.forEach((wf) => {
      const functionName = `workflow_${CodeGenerator.sanitizeIdentifier(wf.fileName)}`;
      const importPath = wf.relativePath.replace(/\.ts$/, '');
      imports += `import { ${functionName} } from '../workflows/${importPath}';\n`;
      importedFunctions.push(functionName);
    });

    let routeDefinitions = '';
    const registeredRoutes = new Set<string>();

    workflows.forEach((wf) => {
      const functionName = `workflow_${CodeGenerator.sanitizeIdentifier(wf.fileName)}`;
      const webhooks = wf.nodes.filter((node) => node.type.includes('webhook'));

      webhooks.forEach((node) => {
        const rawPath = (node.parameters?.path || wf.fileName).toLowerCase().trim().replace(/^\/+/, '');
        const configuredMethod = (node.parameters?.httpMethod || 'POST').toUpperCase();
        const methods = configuredMethod === 'ALL' ? ['GET', 'POST', 'PUT', 'DELETE'] : [configuredMethod];

        const targetPaths = Array.from(new Set([`/${rawPath}`, `/webhook/${rawPath}`]));

        methods.forEach((httpMethod) => {
          targetPaths.forEach((routePath) => {
            const routeKey = `${httpMethod}:${routePath}`;
            if (registeredRoutes.has(routeKey)) return;
            registeredRoutes.add(routeKey);

            routeDefinitions += `
  fastify.route({
    method: '${httpMethod}',
    url: '${routePath}',
    handler: async (request: FastifyRequest, reply: FastifyReply) => {
      const startTime = performance.now();
      const ctx = new WorkflowContext();
      const body = (request.body as Record<string, any>) || {};
      const query = (request.query as Record<string, any>) || {};
      const headers = (request.headers as Record<string, any>) || {};

      const inputItems: N8nItem[] = [{
        json: {
          ...query,
          ...body,
          body,
          query,
          headers,
          __triggerNode: ${JSON.stringify(node.name)},
          __triggerPath: ${JSON.stringify(rawPath)}
        }
      }];

      try {
        const { items, context } = await ${functionName}(inputItems, ctx);
        const durationMs = (performance.now() - startTime).toFixed(2);
        reply.header('X-Compiled-By', 'n8n-to-fastify');
        reply.header('X-Execution-Time', \`\${durationMs}ms\`);

        if (context.webhookResponse) {
          reply.code(context.webhookResponse.statusCode);
          return context.webhookResponse.body;
        }
        reply.code(200);
        return items.length === 1 ? items[0].json : items.map(i => i.json);
      } catch (err: any) {
        request.log.error(err);
        reply.code(500);
        return { error: 'Workflow Execution Error', message: err.message };
      }
    }
  });
`;
          });
        });
      });
    });

    return `${imports}
// Prevent bundler/runtime tree-shaking so subworkflows self-register into WorkflowRegistry
const _registeredWorkflows = [${importedFunctions.join(', ')}];
void _registeredWorkflows;

export async function registerCompiledRoutes(fastify: FastifyInstance): Promise<void> {
${routeDefinitions}
}
`;
  }

  /**
   * Generates the standalone server.ts entry point.
   */
  static generateServerEntry(): string {
    return `import Fastify from 'fastify';
import cors from '@fastify/cors';
import { registerCompiledRoutes } from './routes/index';
import { workflowRegistry } from './runtime/workflow-registry';

const fastify = Fastify({
  logger: true,
});

await fastify.register(cors, { origin: '*' });
await fastify.register(registerCompiledRoutes);

fastify.get('/health', async () => {
  return {
    status: 'ok',
    engine: 'n8n-to-fastify',
    registeredWorkflows: workflowRegistry.listRegistered().length,
  };
});

const port = Number(process.env.PORT || 3000);
const host = process.env.HOST || '0.0.0.0';

try {
  await fastify.listen({ port, host });
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);
} catch (err) {
  fastify.log.error(err);
  process.exit(1);
}
`;
  }

  /**
   * Generates package.json for the compiled standalone microservice.
   */
  static generateOutputPackageJson(dependencies: Record<string, string> = {}): string {
    return JSON.stringify(
      {
        name: 'compiled-n8n-service',
        version: '1.0.0',
        type: 'module',
        scripts: {
          start: 'bun run server.ts',
          dev: 'bun --watch run server.ts',
        },
        dependencies: {
          '@fastify/cors': '^9.0.1',
          fastify: '^4.28.1',
          ...dependencies,
        },
      },
      null,
      2
    );
  }
}
