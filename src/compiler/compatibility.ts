import type { ParsedWorkflow } from './core-parser';
import { getRabbitMqPublisherIssue, rabbitMqPublisherType } from './mappers/rabbitmq';

const supportedNodeTypes = new Set([
  rabbitMqPublisherType,
  ...[
    'webhook',
    'respondToWebhook',
    'code',
    'function',
    'functionItem',
    'executeWorkflow',
    'executeWorkflowTrigger',
    'if',
    'filter',
    'switch',
    'set',
    'httpRequest',
    'aggregate',
    'itemLists',
    'splitOut',
    'merge',
    'postgres',
    'wait',
  ].map(type => `n8n-nodes-base.${type}`),
  ...['openAi', 'googleGemini', 'chainLlm'].map(type => `@n8n/n8n-nodes-langchain.${type}`),
]);

export function getCompatibilityIssues(workflow: ParsedWorkflow): string[] {
  const issues: string[] = [];
  const workflowLabel = JSON.stringify(workflow.name);
  const report = (nodeName: string, issue: string) => {
    issues.push(`Workflow ${workflowLabel}, node ${JSON.stringify(nodeName)}: ${issue}`);
  };

  for (const [source, outputs] of Object.entries(workflow.connections)) {
    for (const type of Object.keys(outputs)) {
      if (type !== 'main') {
        report(source, `connection type ${JSON.stringify(type)} is ignored`);
      }
    }
  }

  for (const node of workflow.nodes) {
    if (node.type === 'n8n-nodes-base.stickyNote') continue;
    if (!supportedNodeTypes.has(node.type)) {
      report(node.name, `unsupported node type ${JSON.stringify(node.type)}`);
      continue;
    }
    if (node.disabled) report(node.name, 'disabled nodes can still be executed');
    if (node.continueOnFail) {
      report(node.name, 'legacy continueOnFail is ignored');
    }
    if (node.onError && node.onError !== 'stopWorkflow') {
      report(node.name, 'the configured onError behavior is ignored');
    }
    if (node.type === rabbitMqPublisherType) {
      const issue = getRabbitMqPublisherIssue(node);
      if (issue) report(node.name, issue);
    }

    const params = node.parameters ?? {};
    if (node.type === 'n8n-nodes-base.httpRequest') {
      if (params.sendBody && params.contentType && params.contentType !== 'json') {
        report(node.name, 'only JSON request bodies are supported');
      }
      if (params.sendHeaders && params.specifyHeaders === 'json') {
        report(node.name, 'JSON header configuration is ignored');
      }
      if (params.authentication && params.authentication !== 'none') {
        report(node.name, 'configured HTTP Request authentication is ignored');
      }
    }
    if (node.type === 'n8n-nodes-base.set' && params.mode === 'raw') {
      report(node.name, 'raw JSON mode is ignored');
    }
    if (node.type === 'n8n-nodes-base.merge' && params.mode === 'combine' &&
      (Number(params.numberInputs) > 2 || (node.incomingConnections?.length ?? 0) > 2)) {
      report(node.name, 'combine mode reads at most two inputs');
    }
  }

  return issues;
}
