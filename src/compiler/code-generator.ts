import { N8nNode, ParsedWorkflow } from './parser';
import { postgresTriggerType } from './postgres-trigger';
import { NodeMappers } from './mappers';
import { mapCommunityNode, selectCommunityAdapters, type CommunityAdapter } from './community-adapters';
import { emitMultiReturnPollingLoop, emitPollingLoop } from './polling-code';
import { getDisabledNodeMode } from './disabled-node';
import { isPositionMerge } from './mappers/position-merge';
import { collectLegacyNodePorts, isCodeNode, usesCodeItemMatching, usesLegacyNodeReferences } from './mappers/code-context';
import { isRawEditFields, usesRawLegacyNodeReferences } from './mappers/raw-edit-fields';
import { rabbitMqPublisherType } from './mappers/rabbitmq';
import { s3NodeType } from './mappers/s3';
import { compressionNodeType } from './mappers/compression';
import { convertToFileType } from './mappers/convert-to-file';
import { extractFromFileType } from './mappers/extract-from-file';
import { stopAndErrorType } from './mappers/stop-and-error';
import { waitNodeType } from './mappers/wait-interval';
import { continuesHttpOnErrorOutput, continuesHttpOnRegularOutput, usesFileResponse, usesJsonHeaders, usesMultipartBody, usesHttpLegacyNodeReferences } from './mappers/http-request';
import { rabbitMqTriggerType } from './rabbitmq-trigger';
import { mapLinkedLlmChain, planLanguageModels } from './mappers/linked-llm';
import { usesNodeRetry } from './mappers/node-retry';

export class CodeGenerator {
  static sanitizeIdentifier(str: string): string {
    return str
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/^[0-9]/, '_$&')
      .replace(/__+/g, '_');
  }

  /**
   * Generates a standalone TypeScript module for a parsed n8n workflow.
   */
  static generateWorkflowCode(wf: ParsedWorkflow, adapters: readonly CommunityAdapter[] = []): string {
    const communityNodes = selectCommunityAdapters(wf, adapters);
    const functionName = `workflow_${this.sanitizeIdentifier(wf.fileName)}`;
    const depth = (wf.relativePath.match(/\//g) || []).length;
    const runtimePrefix = '../'.repeat(depth + 1) + 'runtime';
    const { chains, modelSubnodes } = planLanguageModels(wf);
    const linkedLlmImport = [...chains.values()].some(Boolean)
      ? `import { runLinkedLlmChain } from '${runtimePrefix}/linked-llm';\n`
      : '';
    const httpContinuationImport = wf.nodes.some(node => !node.disabled && (continuesHttpOnRegularOutput(node) || continuesHttpOnErrorOutput(node)))
      ? `import { fetchForRegularOutput, settleHttpItems } from '${runtimePrefix}/http-regular-output';\n`
      : '';
    const httpErrorOutputImport = wf.nodes.some(node => !node.disabled && continuesHttpOnErrorOutput(node))
      ? `import { partitionHttpErrorItems } from '${runtimePrefix}/http-error-output';\n`
      : '';
    const codeHttpRequestImport = wf.nodes.some(isCodeNode)
      ? `import { codeHttpRequest } from '${runtimePrefix}/code-http-request';\n`
      : '';
    const positionMergeImport = wf.nodes.some(isPositionMerge)
      ? `import { mergeByPosition } from '${runtimePrefix}/position-merge';\n`
      : '';
    const communityImports = communityNodes.size > 0 ? [
      `import { executeCommunityAdapter } from '${runtimePrefix}/community-adapter';`,
      ...[...new Set(communityNodes.values())].map(adapter =>
        `import ${adapter.binding} from ${JSON.stringify(`${runtimePrefix}/adapters/${adapter.fileName}`)};`),
      '',
    ].join('\n') : '';
    const legacyNodes = new Set(wf.nodes.filter(node =>
      usesLegacyNodeReferences(node) || usesRawLegacyNodeReferences(node) || usesHttpLegacyNodeReferences(node)
    ));
    const rawFieldsImport = wf.nodes.some(isRawEditFields)
      ? `import { applyRawEditFields, encodeJsonTemplateValue } from '${runtimePrefix}/raw-edit-fields';\n`
      : '';
    const legacyImport = legacyNodes.size > 0
      ? `import { createLegacyNodeReferences } from '${runtimePrefix}/code-context';\n`
      : '';
    const retryNodes = wf.nodes.filter(node => !node.disabled && usesNodeRetry(node, Boolean(chains.get(node))));
    const retryImport = retryNodes.length > 0
      ? `import { runWithNodeRetry${retryNodes.some(node => node.type === 'n8n-nodes-base.httpRequest') ? ', settleRetryRequests' : ''} } from '${runtimePrefix}/node-retry';\n`
      : '';
    const legacyNames = legacyNodes.size > 0
      ? `const legacyNodeNames = new Set<string>(${JSON.stringify(wf.nodes.map(node => node.name))});\n`
      : '';
    const nodesByName = new Map(wf.nodes.map(node => [node.name, node]));
    const batchLookup = wf.nodes.some(node => !node.disabled && usesCodeItemMatching(node))
      ? 'getItemMatchingBatches' : 'getNodeOutputBatches';
    const rabbitMqImport = wf.nodes.some((node) => node.type === rabbitMqPublisherType)
      ? `import { publishRabbitMq } from '${runtimePrefix}/rabbitmq';\n`
      : '';
    const s3Import = wf.nodes.some(node => node.type === s3NodeType)
      ? `import { downloadS3Files } from '${runtimePrefix}/s3';\n`
      : '';
    const compressionImport = wf.nodes.some(node => node.type === compressionNodeType)
      ? `import { decompressItems } from '${runtimePrefix}/compression';\n`
      : '';
    const jsonFileImport = wf.nodes.some(node => node.type === convertToFileType)
      ? `import { convertItemsToJsonFile } from '${runtimePrefix}/convert-to-json';\n`
      : '';
    const extractFileImport = wf.nodes.some(node => node.type === extractFromFileType)
      ? `import { extractBase64Items } from '${runtimePrefix}/extract-from-file';\n`
      : '';
    const httpRequestImport = wf.nodes.some(node =>
      node.type.includes('httpRequest') &&
      (usesMultipartBody(node.parameters) || usesJsonHeaders(node.parameters) || usesFileResponse(node.parameters))
    ) ? `import { createRequestForm, parseRequestHeaders, readFileResponse } from '${runtimePrefix}/http-request';\n` : '';
    const waitImport = wf.nodes.some(node => node.type === waitNodeType)
      ? `import { waitForInterval } from '${runtimePrefix}/wait-interval';\n`
      : '';
    const stopAndErrorImport = wf.nodes.some(node => node.type === stopAndErrorType)
      ? `import { createWorkflowStopError } from '${runtimePrefix}/stop-and-error';\n`
      : '';
    const jsonBodyImport = wf.nodes.some(node =>
      node.type.includes('httpRequest') && node.parameters?.jsonBody && !usesMultipartBody(node.parameters) &&
      ['POST', 'PUT', 'PATCH'].includes((node.parameters.method ?? 'GET').toUpperCase())
    ) ? `import { encodeJsonRequestBody } from '${runtimePrefix}/http-request';\n` : '';

    const httpStatusImport = wf.nodes.some(node => node.type.includes('httpRequest'))
      ? `import { fetchWithStatus } from '${runtimePrefix}/http-request';\n`
      : '';

    const httpTimeoutImport = wf.nodes.some(node => node.type.includes('httpRequest'))
      ? `import { createResponseTimeout } from '${runtimePrefix}/http-request';\n`
      : '';

    const imports = `${codeHttpRequestImport}import { WorkflowContext } from '${runtimePrefix}/workflow-context';
import { n8n, N8nItem } from '${runtimePrefix}/n8n-helper';
${communityImports}import { executeSubworkflow, registerWorkflow } from '${runtimePrefix}/workflow-registry';
${httpStatusImport}${positionMergeImport}${legacyImport}${rawFieldsImport}${rabbitMqImport}${s3Import}${compressionImport}${jsonFileImport}${httpRequestImport}${waitImport}${stopAndErrorImport}
${httpTimeoutImport}${jsonBodyImport}${extractFileImport}${linkedLlmImport}${legacyNames}
${retryImport}const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`;

    let declCode = '  let item: N8nItem = inputItems[0] || { json: {} };\n';
    if (wf.useCanvasOrder) declCode += '  let workflowItems = inputItems;\n';
    let bodyCode = '  ctx.setNodeOutput("__initial__", inputItems);\n';

    const emitNodeBlock = (node: N8nNode, condition: string, code: string) => {
      // Record the result in this invocation, even when the execution context is shared.
      if (wf.useCanvasOrder && (node.type !== stopAndErrorType || node.disabled)) {
        code += `\nworkflowItems = ctx.getNode(${JSON.stringify(node.name)}).all();`;
      }
      return this.generateNodeBlock(wf.id, node, condition, code);
    };

    const nodeVarMap = new Map<string, string>();
    wf.orderedNodes.forEach((node, index) => {
      const varName = `node_${index}_${this.sanitizeIdentifier(node.name)}`;
      nodeVarMap.set(node.name, varName);
    });

    const triggers = wf.triggers;
    const hasEventTrigger = triggers.some(node =>
      node.type === rabbitMqTriggerType || (node.type === postgresTriggerType && !node.disabled)
    );
    const disabledModes = new Map(wf.orderedNodes.map(node => [node, getDisabledNodeMode(node, wf)]));
    const nonTriggers = wf.orderedNodes.filter((n) => !triggers.includes(n) && !modelSubnodes.has(n) && disabledModes.get(n) !== 'skip');
    const loops = wf.pollingLoops ?? [];
    const loopsByMember = new Map(loops.flatMap(loop => loop.nodes.map(name => [name, loop] as const)));
    const terminalNames = new Set(loops.flatMap(loop => 'terminalExits' in loop ? loop.terminalExits.map(exit => exit.target) : []));
    const mapNode = (node: N8nNode, previousVariableName: string) => {
      const varName = nodeVarMap.get(node.name)!;
      if (disabledModes.get(node) === 'passthrough') return `
    ${varName} = ${previousVariableName} || [];
    item = ${varName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(node.name)}, ${varName});`;
      const adapter = communityNodes.get(node);
      if (adapter) return mapCommunityNode(node, adapter, varName, previousVariableName);
      if (chains.has(node)) {
        return mapLinkedLlmChain({ node, workflowName: wf.name, variableName: varName, previousVariableName }, chains.get(node) ?? null);
      }
      return NodeMappers.mapNode({
        node,
        workflowName: wf.name,
        variableName: varName,
        previousVariableName,
        legacyNodePorts: legacyNodes.has(node) ? collectLegacyNodePorts(node, nodesByName) : undefined,
      });
    };

    triggers.forEach((node) => {
      if (node.type === postgresTriggerType && node.disabled) return;
      const index = wf.orderedNodes.indexOf(node);
      const varName = nodeVarMap.get(node.name)!;
      declCode += `  let ${varName}: any = inputItems;\n`;

      const nodeName = JSON.stringify(node.name);
      let condition = `entryNode === ${nodeName}`;
      if (node.type !== rabbitMqTriggerType && node.type !== postgresTriggerType) {
        const rawNodePath = (node.parameters?.path || node.name).toLowerCase().trim().replace(/^\/+/, '');
        bodyCode += `  const triggerNode_${index} = item?.json?.__triggerNode;\n`;
        bodyCode += `  const triggerPath_${index} = item?.json?.__triggerPath;\n`;
        condition = `entryNode === undefined && (!triggerNode_${index} || triggerNode_${index} === ${nodeName} || triggerPath_${index} === ${JSON.stringify(rawNodePath)})`;
      }
      const nodeCode = `${varName} = inputItems;\nctx.setNodeOutput(${nodeName}, inputItems);`;
      bodyCode += emitNodeBlock(node, condition, nodeCode);
    });

    nonTriggers.forEach((node) => {
      const varName = nodeVarMap.get(node.name)!;
      declCode += `  let ${varName}: any = inputItems;\n`;

      const loop = loopsByMember.get(node.name);
      if (loop) {
        if (node.name === loop.entry) {
          bodyCode += 'routes' in loop
            ? emitMultiReturnPollingLoop(loop, nodesByName, mapNode, emitNodeBlock)
            : emitPollingLoop(loop, nodesByName, mapNode, emitNodeBlock);
        }
        return;
      }
      if (terminalNames.has(node.name)) return;

      let branchCheckStr = 'true';
      const incoming = node.incomingConnections ?? [];
      // Registered adapters receive connected batches even when their type contains "merge".
      const runPerIncomingBatch = incoming.length > 0 &&
        (disabledModes.get(node) === 'passthrough' || communityNodes.has(node) || !node.type.includes('merge'));

      if (incoming.length > 0) {
        const branchChecks = incoming.map(
          (conn) => `ctx.isBranchActive(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex})`
        );
        branchCheckStr = `(${branchChecks.join(' || ')})`;
      }

      const prevExpr = runPerIncomingBatch ? 'incomingItems' : 'inputItems';
      let nodeCode = mapNode(node, prevExpr);
      if (runPerIncomingBatch) {
        const emittedRegions = new Set<string>();
        const inputBatches = incoming.flatMap(conn => {
          const region = loopsByMember.get(conn.sourceNode);
          if (!region || !('routes' in region)) {
            return [`ctx.${batchLookup}(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex})`];
          }
          if (emittedRegions.has(region.entry)) return [];
          emittedRegions.add(region.entry);
          return [`ctx.getPollingExitBatches(${JSON.stringify(region.entry)}, ${JSON.stringify(node.name)})`];
        });
        // Keep the mapper body's original indentation, including multiline literals.
        nodeCode = `await ctx.runNodeBatches(${JSON.stringify(node.name)}, [${inputBatches.join(', ')}].flat(), async (incomingItems) => {
${nodeCode.trim()}
});`;
      }

      const ancestorTriggers = node.ancestorTriggers ?? [];
      if (ancestorTriggers.length > 0) {
        const triggerConditions = ancestorTriggers
          .map((t) => `ctx.hasExecutedNode(${JSON.stringify(t)})`)
          .join(' || ');
        branchCheckStr = `(${triggerConditions}) && ${branchCheckStr}`;
      } else if (hasEventTrigger) {
        branchCheckStr = `entryNode === undefined && ${branchCheckStr}`;
      }
      bodyCode += emitNodeBlock(node, branchCheckStr, nodeCode);
    });

    const lastNodeName = wf.orderedNodes
      .filter(node => !modelSubnodes.has(node) && disabledModes.get(node) !== 'skip' && (node.type !== stopAndErrorType || node.disabled))
      .at(-1)?.name;
    const returnItemsExpr = wf.useCanvasOrder ? 'workflowItems' : lastNodeName
      ? `(ctx.getNode(${JSON.stringify(lastNodeName)})?.all() || [{ json: item.json }])`
      : `inputItems`;

    const keys = Array.from(new Set([wf.id, wf.name, wf.fileName])).filter(Boolean);

    return `${httpContinuationImport}${httpErrorOutputImport}${imports}
export async function ${functionName}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext,
  entryNode?: string
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
  const $env = process.env;
${declCode}
${bodyCode}
  const finalItems = ${returnItemsExpr};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(keys)}, ${functionName});
`;
  }

  private static generateNodeBlock(
    workflowId: string,
    node: N8nNode,
    condition: string,
    nodeCode: string,
  ): string {
    const traceArgs = [workflowId, node.id || node.name, node.name].map(value => JSON.stringify(value)).join(', ');
    // Prefix the block without changing whitespace inside multiline JavaScript literals.
    const body = nodeCode.trim();
    return `  if (${condition}) {
    const finishTrace = ctx.startNodeTrace(${traceArgs});
    try {
      ${body}
    } catch (error) {
      finishTrace?.('error');
      throw error;
    }
    finishTrace?.('completed');
  } else {
    ctx.skipNodeTrace(${traceArgs});
  }
`;
  }
}
