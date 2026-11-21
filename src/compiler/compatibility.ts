import type { ParsedWorkflow } from './core-parser';
import { planLanguageModels } from './mappers/linked-llm';
import { usesNodeRetry } from './mappers/node-retry';
import { selectCommunityAdapters, type CommunityAdapter } from './community-adapters';
import { getDisabledNodeMode } from './disabled-node';
import { getPositionMergeIssue, isPositionMerge } from './mappers/position-merge';
import { getCodeItemMatchingIssue, getLegacyNodeReferenceIssue, usesCodeItemMatching } from './mappers/code-context';
import { continuesCodeOnRegularOutput } from './mappers/code-errors';
import { getRawEditFieldsIssue } from './mappers/raw-edit-fields';
import { getRabbitMqPublisherIssue, rabbitMqPublisherType } from './mappers/rabbitmq';
import { getS3DownloadIssue, s3NodeType } from './mappers/s3';
import { compressionNodeType, getCompressionIssue } from './mappers/compression';
import { convertToFileType, getConvertToFileIssue } from './mappers/convert-to-file';
import { extractFromFileType, getExtractFromFileIssue } from './mappers/extract-from-file';
import { continuesHttpOnErrorOutput, continuesHttpOnRegularOutput, getMultipartIssue, usesMultipartBody } from './mappers/http-request';
import { getWaitIntervalIssue, waitNodeType } from './mappers/wait-interval';
import { getStopAndErrorIssue, stopAndErrorType } from './mappers/stop-and-error';
import { parseRabbitMqTrigger, rabbitMqTriggerType } from './rabbitmq-trigger';
import { parsePostgresTrigger, postgresTriggerType } from './postgres-trigger';

const supportedNodeTypes = new Set([
  compressionNodeType,
  convertToFileType,
  extractFromFileType,
  rabbitMqPublisherType,
  rabbitMqTriggerType,
  postgresTriggerType,
  s3NodeType,
  stopAndErrorType,
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
    'noOp',
  ].map(type => `n8n-nodes-base.${type}`),
  ...['openAi', 'googleGemini', 'chainLlm'].map(type => `@n8n/n8n-nodes-langchain.${type}`),
]);

export function getCompatibilityIssues(workflow: ParsedWorkflow, adapters: readonly CommunityAdapter[] = []): string[] {
  const issues: string[] = [];
  const selections = selectCommunityAdapters(workflow, adapters);
  const adapterTypes = new Set(adapters.map(adapter => adapter.type));
  const workflowLabel = JSON.stringify(workflow.name);
  const report = (nodeName: string, issue: string) => {
    issues.push(`Workflow ${workflowLabel}, node ${JSON.stringify(nodeName)}: ${issue}`);
  };

  const { modelSubnodes, issues: modelIssues } = planLanguageModels(workflow);
  for (const [nodeName, issue] of modelIssues) report(nodeName, issue);
  for (const [source, outputs] of Object.entries(workflow.connections)) {
    for (const type of Object.keys(outputs)) {
      if (type !== 'main' && type !== 'ai_languageModel') {
        report(source, `connection type ${JSON.stringify(type)} is ignored`);
      }
    }
  }

  for (const node of workflow.nodes) {
    if (node.type === 'n8n-nodes-base.stickyNote' || modelSubnodes.has(node)) continue;
    if ((node.type === rabbitMqTriggerType || node.type === postgresTriggerType) && node.disabled) continue;
    if (node.disabled) {
      if (!getDisabledNodeMode(node, workflow)) report(node.name, 'disabled node configuration is unsupported');
      continue;
    }
    const adapter = selections.get(node);
    if (!supportedNodeTypes.has(node.type) && !adapter) {
      if (!adapterTypes.has(node.type)) report(node.name, `unsupported node type ${JSON.stringify(node.type)}`);
      else if (workflow.triggers.includes(node)) report(node.name, 'community node adapters support action nodes only');
      else report(node.name, `no community adapter for ${JSON.stringify(node.type)} at typeVersion ${node.typeVersion}`);
      continue;
    }
    if (!adapter && node.retryOnFail === true && !usesNodeRetry(node)) {
      report(node.name, 'Retry On Fail is only supported for HTTP Request and S3 Download');
    }
    if (adapter && node.retryOnFail === true && !adapter.handlesRetries) {
      report(node.name, 'the community adapter does not handle Retry On Fail');
    }
    const mainOutputs: unknown = workflow.connections[node.name]?.main;
    if (adapter && Array.isArray(mainOutputs) && mainOutputs.slice(1).some((targets: unknown) =>
      Array.isArray(targets) && targets.length > 0
    )) {
      report(node.name, 'community adapters support only main output 0');
    }
    if (node.continueOnFail) {
      report(node.name, 'legacy continueOnFail is ignored');
    }
    if (node.onError && node.onError !== 'stopWorkflow' &&
        !continuesCodeOnRegularOutput(node) && !continuesHttpOnRegularOutput(node) && !continuesHttpOnErrorOutput(node)) {
      report(node.name, 'the configured onError behavior is ignored');
    }
    if (node.type === rabbitMqPublisherType) {
      const issue = getRabbitMqPublisherIssue(node);
      if (issue) report(node.name, issue);
    }
    if (node.type === rabbitMqTriggerType) {
      const options = parseRabbitMqTrigger(node);
      if (typeof options === 'string') report(node.name, options);
    }
    if (node.type === 'n8n-nodes-base.noOp' && (node.typeVersion ?? 1) !== 1) {
      report(node.name, 'No Operation needs typeVersion 1');
    }

    const params = node.parameters ?? {};
    const codeIssue = getLegacyNodeReferenceIssue(node);
    if (codeIssue) report(node.name, codeIssue);
    const itemMatchingIssue = getCodeItemMatchingIssue(node);
    if (itemMatchingIssue) report(node.name, itemMatchingIssue);
    else if (usesCodeItemMatching(node) && workflow.pollingLoops?.some(loop => loop.nodes.includes(node.name))) {
      report(node.name, 'Code itemMatching is unsupported inside a polling loop');
    }
    if (node.type === waitNodeType) {
      const issue = getWaitIntervalIssue(node);
      if (issue) report(node.name, issue);
    }
    if (node.type === stopAndErrorType) {
      const issue = getStopAndErrorIssue(node);
      if (issue) report(node.name, issue);
    }
    if (node.type === compressionNodeType) {
      const issue = getCompressionIssue(node);
      if (issue) report(node.name, issue);
    }
    if (node.type === convertToFileType) {
      const issue = getConvertToFileIssue(node);
      if (issue) report(node.name, issue);
    }
    if (node.type === extractFromFileType) {
      const issue = getExtractFromFileIssue(node);
      if (issue) report(node.name, issue);
    }
    if (node.type === s3NodeType) {
      const issue = getS3DownloadIssue(node);
      if (issue) report(node.name, issue);
    }
    if (node.type === postgresTriggerType) {
      const options = parsePostgresTrigger(node);
      if (typeof options === 'string') report(node.name, options);
    }
    if (node.type === 'n8n-nodes-base.httpRequest') {
      if (usesMultipartBody(params)) {
        const issue = getMultipartIssue(params);
        if (issue) report(node.name, issue);
      } else if (params.sendBody && params.contentType && params.contentType !== 'json') {
        report(node.name, 'only JSON and multipart request bodies are supported');
      }
      if (params.authentication && params.authentication !== 'none') {
        report(node.name, 'configured HTTP Request authentication is ignored');
      }
    }
    const rawIssue = getRawEditFieldsIssue(node);
    if (rawIssue) report(node.name, rawIssue);
    if (isPositionMerge(node)) {
      const issue = getPositionMergeIssue(node);
      if (issue) report(node.name, issue);
    } else if (node.type === 'n8n-nodes-base.merge' && params.mode === 'combine' &&
      (Number(params.numberInputs) > 2 || (node.incomingConnections?.length ?? 0) > 2)) {
      report(node.name, 'combine mode reads at most two inputs');
    }
  }

  return issues;
}
