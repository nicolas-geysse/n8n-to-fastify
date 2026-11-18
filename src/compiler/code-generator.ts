import { N8nNode, ParsedWorkflow } from './parser';
import { NodeMappers } from './mappers';
import { isPositionMerge } from './mappers/position-merge';
import { collectLegacyNodePorts, usesLegacyNodeReferences } from './mappers/code-context';
import { s3NodeType } from './mappers/s3';
import { usesJsonHeaders, usesMultipartBody } from './mappers/http-request';
import { rabbitMqPublisherType } from './mappers/rabbitmq';
import { rabbitMqTriggerType } from './rabbitmq-trigger';
import { convertToFileType } from './mappers/convert-to-file';
import { isRawEditFields, usesRawLegacyNodeReferences } from './mappers/raw-edit-fields';

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
  static generateWorkflowCode(wf: ParsedWorkflow): string {
    const functionName = `workflow_${this.sanitizeIdentifier(wf.fileName)}`;
    const depth = (wf.relativePath.match(/\//g) || []).length;
    const runtimePrefix = '../'.repeat(depth + 1) + 'runtime';
    const positionMergeImport = wf.nodes.some(isPositionMerge)
      ? `import { mergeByPosition } from '${runtimePrefix}/position-merge';\n`
      : '';
    const legacyNodes = new Set(wf.nodes.filter(node =>
      usesLegacyNodeReferences(node) || usesRawLegacyNodeReferences(node)
    ));
    const rawFieldsImport = wf.nodes.some(isRawEditFields)
      ? `import { applyRawEditFields, encodeJsonTemplateValue } from '${runtimePrefix}/raw-edit-fields';\n`
      : '';
    const legacyImport = legacyNodes.size > 0
      ? `import { createLegacyNodeReferences } from '${runtimePrefix}/code-context';\n`
      : '';
    const legacyNames = legacyNodes.size > 0
      ? `const legacyNodeNames = new Set<string>(${JSON.stringify(wf.nodes.map(node => node.name))});\n`
      : '';
    const nodesByName = new Map(wf.nodes.map(node => [node.name, node]));
    const s3Import = wf.nodes.some(node => node.type === s3NodeType)
      ? `import { downloadS3Files } from '${runtimePrefix}/s3';\n`
      : '';
    const httpRequestImport = wf.nodes.some(node =>
      node.type.includes('httpRequest') && (usesMultipartBody(node.parameters) || usesJsonHeaders(node.parameters))
    ) ? `import { createRequestForm, parseRequestHeaders } from '${runtimePrefix}/http-request';\n` : '';
    const rabbitMqImport = wf.nodes.some((node) => node.type === rabbitMqPublisherType)
      ? `import { publishRabbitMq } from '${runtimePrefix}/rabbitmq';\n`
      : '';
    const jsonFileImport = wf.nodes.some(node => node.type === convertToFileType)
      ? `import { convertItemsToJsonFile } from '${runtimePrefix}/convert-to-json';\n`
      : '';

    const imports = `import { WorkflowContext } from '${runtimePrefix}/workflow-context';
import { n8n, N8nItem } from '${runtimePrefix}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${runtimePrefix}/workflow-registry';
${positionMergeImport}${legacyImport}${s3Import}${httpRequestImport}${rabbitMqImport}${jsonFileImport}${rawFieldsImport}
${legacyNames}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`;

    let declCode = '  let item: N8nItem = inputItems[0] || { json: {} };\n';
    let bodyCode = '  ctx.setNodeOutput("__initial__", inputItems);\n';

    const nodeVarMap = new Map<string, string>();
    wf.orderedNodes.forEach((node, index) => {
      const varName = `node_${index}_${this.sanitizeIdentifier(node.name)}`;
      nodeVarMap.set(node.name, varName);
    });

    const triggers = wf.triggers;
    const hasRabbitMqTrigger = triggers.some(node => node.type === rabbitMqTriggerType);
    const nonTriggers = wf.orderedNodes.filter((n) => !triggers.includes(n));

    triggers.forEach((node) => {
      const index = wf.orderedNodes.indexOf(node);
      const varName = nodeVarMap.get(node.name)!;
      declCode += `  let ${varName}: any = inputItems;\n`;

      const nodeName = JSON.stringify(node.name);
      let condition = `entryNode === ${nodeName}`;
      if (node.type !== rabbitMqTriggerType) {
        const rawNodePath = (node.parameters?.path || node.name).toLowerCase().trim().replace(/^\/+/, '');
        bodyCode += `  const triggerNode_${index} = item?.json?.__triggerNode;\n`;
        bodyCode += `  const triggerPath_${index} = item?.json?.__triggerPath;\n`;
        condition = `entryNode === undefined && (!triggerNode_${index} || triggerNode_${index} === ${nodeName} || triggerPath_${index} === ${JSON.stringify(rawNodePath)})`;
      }
      const nodeCode = `${varName} = inputItems;\nctx.setNodeOutput(${nodeName}, inputItems);`;
      bodyCode += this.generateNodeBlock(wf.id, node, condition, nodeCode);
    });

    nonTriggers.forEach((node) => {
      const varName = nodeVarMap.get(node.name)!;
      declCode += `  let ${varName}: any = inputItems;\n`;

      let branchCheckStr = 'true';
      const incoming = node.incomingConnections ?? [];
      const runPerIncomingBatch = incoming.length > 0 && !node.type.includes('merge');

      if (incoming.length > 0) {
        const branchChecks = incoming.map(
          (conn) => `ctx.isBranchActive(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex})`
        );
        branchCheckStr = `(${branchChecks.join(' || ')})`;
      }

      let nodeCode = NodeMappers.mapNode({
        node,
        workflowName: wf.name,
        variableName: varName,
        previousVariableName: runPerIncomingBatch ? 'incomingItems' : 'inputItems',
        legacyNodePorts: legacyNodes.has(node) ? collectLegacyNodePorts(node, nodesByName) : undefined,
      });
      if (runPerIncomingBatch) {
        const batches = incoming.map(
          conn => `ctx.getNodeOutputBatches(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex})`
        );
        // Keep the mapper body's original indentation, including multiline literals.
        nodeCode = `await ctx.runNodeBatches(${JSON.stringify(node.name)}, [${batches.join(', ')}].flat(), async (incomingItems) => {
${nodeCode.trim()}
});`;
      }

      const ancestorTriggers = node.ancestorTriggers ?? [];
      if (ancestorTriggers.length > 0) {
        const triggerConditions = ancestorTriggers
          .map((t) => `ctx.hasExecutedNode(${JSON.stringify(t)})`)
          .join(' || ');
        branchCheckStr = `(${triggerConditions}) && ${branchCheckStr}`;
      } else if (hasRabbitMqTrigger) {
        branchCheckStr = `entryNode === undefined && ${branchCheckStr}`;
      }
      bodyCode += this.generateNodeBlock(wf.id, node, branchCheckStr, nodeCode,
        ancestorTriggers.length > 0 || hasRabbitMqTrigger ? '      ' : '    ');
    });

    const lastNodeName = wf.orderedNodes[wf.orderedNodes.length - 1]?.name;
    const returnItemsExpr = lastNodeName
      ? `(ctx.getNode(${JSON.stringify(lastNodeName)})?.all() || [{ json: item.json }])`
      : `inputItems`;

    const keys = Array.from(new Set([wf.id, wf.name, wf.fileName])).filter(Boolean);

    return `${imports}
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
    nodeIndent = '    ',
  ): string {
    const traceArgs = [workflowId, node.id || node.name, node.name].map(value => JSON.stringify(value)).join(', ');
    // Keep the previous indentation because it also affects multiline JavaScript literals.
    const body = nodeIndent + nodeCode.trim().replace(/\n/g, `\n${nodeIndent}`);
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
