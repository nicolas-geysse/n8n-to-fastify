import type { N8nNode } from '../core-parser';
import { ExpressionParser } from '../expression-parser';
import { getWaitDelayMs } from '../../runtime/wait-interval';
import type { NodeMapperContext } from './index';

export const waitNodeType = 'n8n-nodes-base.wait';

function intervalParameters(node: N8nNode): { amount: unknown; unit: unknown } {
  const params = node.parameters ?? {};
  return {
    amount: params.amount ?? (node.typeVersion === 1 ? 1 : 5),
    unit: params.unit ?? (node.typeVersion === 1 ? 'hours' : 'seconds'),
  };
}

function isExpression(value: unknown): boolean {
  return typeof value === 'string' && value.trimStart().startsWith('=') && value.includes('{{');
}

export function getWaitIntervalIssue(node: N8nNode): string | undefined {
  const version = node.typeVersion ?? 1.1;
  if ((version !== 1 && version !== 1.1) || (node.parameters?.resume ?? 'timeInterval') !== 'timeInterval') {
    return 'Wait supports the After Time Interval mode on node versions 1 and 1.1';
  }
  const { amount, unit } = intervalParameters(node);
  try {
    getWaitDelayMs(isExpression(amount) ? 0 : amount, isExpression(unit) ? 'seconds' : unit);
  } catch (error) {
    return (error as Error).message;
  }
}

export function mapWaitInterval(ctx: NodeMapperContext): string {
  const issue = getWaitIntervalIssue(ctx.node);
  if (issue) throw new Error(`Node ${JSON.stringify(ctx.node.name)}: ${issue}`);

  const { amount, unit } = intervalParameters(ctx.node);
  const previous = ctx.previousVariableName ?? 'inputItems';
  return `
    ${ctx.variableName} = ${previous} || inputItems;
    if (${ctx.variableName}.length > 0) {
      const inputItems: N8nItem[] = ${ctx.variableName};
      const item = inputItems[0];
      await waitForInterval(${ExpressionParser.parse(amount)}, ${ExpressionParser.parse(unit)});
    }
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
}
