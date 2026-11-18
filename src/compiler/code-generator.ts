import { N8nNode, ParsedWorkflow } from './parser';
import { NodeMappers } from './mappers';

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

    const imports = `import { WorkflowContext } from '${runtimePrefix}/workflow-context';
import { n8n, N8nItem } from '${runtimePrefix}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${runtimePrefix}/workflow-registry';

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

    const triggers = wf.orderedNodes.filter(
      (n) => n.type.includes('webhook') || n.type.includes('trigger') || n.type.includes('cron')
    );
    const nonTriggers = wf.orderedNodes.filter((n) => !triggers.includes(n));

    triggers.forEach((node) => {
      const index = wf.orderedNodes.indexOf(node);
      const varName = nodeVarMap.get(node.name)!;
      declCode += `  let ${varName}: any = inputItems;\n`;

      const rawNodePath = (node.parameters?.path || node.name).toLowerCase().trim().replace(/^\/+/, '');
      bodyCode += `  const triggerNode_${index} = item?.json?.__triggerNode;\n`;
      bodyCode += `  const triggerPath_${index} = item?.json?.__triggerPath;\n`;
      const condition = `!triggerNode_${index} || triggerNode_${index} === ${JSON.stringify(node.name)} || triggerPath_${index} === ${JSON.stringify(rawNodePath)}`;
      const nodeCode = `${varName} = inputItems;\nctx.setNodeOutput(${JSON.stringify(node.name)}, inputItems);`;
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
      }
      bodyCode += this.generateNodeBlock(wf.id, node, branchCheckStr, nodeCode,
        ancestorTriggers.length > 0 ? '      ' : '    ');
    });

    const lastNodeName = wf.orderedNodes[wf.orderedNodes.length - 1]?.name;
    const returnItemsExpr = lastNodeName
      ? `(ctx.getNode(${JSON.stringify(lastNodeName)})?.all() || [{ json: item.json }])`
      : `inputItems`;

    const keys = Array.from(new Set([wf.id, wf.name, wf.fileName])).filter(Boolean);

    return `${imports}
export async function ${functionName}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
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
