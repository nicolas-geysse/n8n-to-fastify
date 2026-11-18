import type { N8nNode } from '../core-parser';
import { ExpressionParser } from '../expression-parser';
import type { NodeMapperContext } from './index';

export const stopAndErrorType = 'n8n-nodes-base.stopAndError';

export function getStopAndErrorIssue(node: N8nNode): string | undefined {
  if ((node.typeVersion ?? 1) !== 1) return 'Stop and Error supports node version 1';
  const type = node.parameters?.errorType ?? 'errorMessage';
  if (type !== 'errorMessage' && type !== 'errorObject') {
    return 'Stop and Error supports Error Message and Error Object';
  }
}

export function mapStopAndError(ctx: NodeMapperContext): string {
  const issue = getStopAndErrorIssue(ctx.node);
  if (issue) throw new Error(`Node ${JSON.stringify(ctx.node.name)}: ${issue}`);
  const params = ctx.node.parameters ?? {};
  const type = params.errorType ?? 'errorMessage';
  const configured = params[type] ?? '';
  const value = ExpressionParser.parse(typeof configured === 'string' && !configured.includes('{{')
    ? configured.replace(/^=/, '') : configured);
  const input = ctx.previousVariableName ?? 'inputItems';
  return `
    {
      const source: N8nItem[] = ${input} || inputItems;
      if (source.length > 0) {
        const inputItems = source;
        const item = inputItems[0];
        throw createWorkflowStopError(${JSON.stringify(type)}, ${value});
      }
    }`;
}
