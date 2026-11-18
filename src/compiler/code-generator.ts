import { ParsedWorkflow } from './parser';
import { NodeMappers } from './mappers';
import { stopAndErrorType } from './mappers/stop-and-error';

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
    const stopAndErrorImport = wf.nodes.some(node => node.type === stopAndErrorType)
      ? `import { createWorkflowStopError } from '${runtimePrefix}/stop-and-error';\n`
      : '';

    const imports = `import { WorkflowContext } from '${runtimePrefix}/workflow-context';
import { n8n, N8nItem } from '${runtimePrefix}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${runtimePrefix}/workflow-registry';
${stopAndErrorImport}

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`;

    let declCode = '  let item: N8nItem = inputItems[0] || { json: {} };\n';
    let bodyCode = '  ctx.setNodeOutput("__initial__", inputItems);\n';
    let prevVarName = 'inputItems';

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
      bodyCode += `  if (!triggerNode_${index} || triggerNode_${index} === ${JSON.stringify(node.name)} || triggerPath_${index} === ${JSON.stringify(rawNodePath)}) {\n`;
      bodyCode += `    ${varName} = inputItems;\n`;
      bodyCode += `    ctx.setNodeOutput(${JSON.stringify(node.name)}, inputItems);\n`;
      bodyCode += `  }\n`;
    });

    nonTriggers.forEach((node) => {
      const varName = nodeVarMap.get(node.name)!;
      declCode += `  let ${varName}: any = inputItems;\n`;

      let actualPrevVar = `inputItems`;
      let branchCheckStr = 'true';

      if (node.incomingConnections && node.incomingConnections.length > 0) {
        if (node.incomingConnections.length === 1) {
          const conn = node.incomingConnections[0];
          actualPrevVar = `(ctx.isBranchActive(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex}) ? ctx.getNode(${JSON.stringify(conn.sourceNode)})?.all() : null)`;
        } else {
          const choices = node.incomingConnections.map(
            (conn) =>
              `(ctx.isBranchActive(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex}) ? ctx.getNode(${JSON.stringify(conn.sourceNode)})?.all() : null)`
          );
          actualPrevVar = `(${choices.join(' || ')})`;
        }

        const branchChecks = node.incomingConnections.map(
          (conn) => `ctx.isBranchActive(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex})`
        );
        branchCheckStr = `(${branchChecks.join(' || ')})`;
      }

      const nodeCode = NodeMappers.mapNode({
        node,
        workflowName: wf.name,
        variableName: varName,
        previousVariableName: actualPrevVar,
      });

      const finalExecutionBlock = `  if (${branchCheckStr}) {\n    ${nodeCode.trim().replace(/\n/g, '\n    ')}\n  }\n`;

      if (node.ancestorTriggers && node.ancestorTriggers.length > 0) {
        const triggerConditions = node.ancestorTriggers
          .map((t) => `ctx.hasExecutedNode(${JSON.stringify(t)})`)
          .join(' || ');
        bodyCode += `  if (${triggerConditions}) {\n  ${finalExecutionBlock.trim().replace(/\n/g, '\n  ')}\n  }\n`;
      } else {
        bodyCode += finalExecutionBlock;
      }

      prevVarName = node.name;
    });

    const lastNodeName = wf.orderedNodes.filter(node => node.type !== stopAndErrorType || node.disabled).at(-1)?.name;
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
${declCode}
${bodyCode}
  const finalItems = ${returnItemsExpr};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(keys)}, ${functionName});
`;
  }
}
