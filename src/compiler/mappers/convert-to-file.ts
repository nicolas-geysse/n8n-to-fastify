import type { N8nNode } from '../core-parser';
import { ExpressionParser } from '../expression-parser';
import type { NodeMapperContext } from './index';

export const convertToFileType = 'n8n-nodes-base.convertToFile';

export function getConvertToFileIssue(node: N8nNode): string | undefined {
  const params = node.parameters ?? {};
  const version = node.typeVersion ?? 1;
  if ((version !== 1 && version !== 1.1) || params.operation !== 'toJson') {
    return 'Convert to File supports Convert to JSON on node versions 1 and 1.1';
  }
  if ((params.mode ?? 'once') !== 'once') {
    return 'Convert to JSON supports the All Items to One File mode';
  }
  if (!['utf8', 'utf-8'].includes(params.options?.encoding ?? 'utf8')) {
    return 'Convert to JSON supports UTF-8 encoding';
  }
}

export function mapConvertToFile(ctx: NodeMapperContext): string {
  const issue = getConvertToFileIssue(ctx.node);
  if (issue) throw new Error(`Node ${JSON.stringify(ctx.node.name)}: ${issue}`);

  const params = ctx.node.parameters ?? {};
  const options = params.options ?? {};
  const source = ctx.previousVariableName ?? 'inputItems';
  const binaryProperty = ExpressionParser.parse(params.binaryPropertyName ?? 'data');
  const fileName = ExpressionParser.parse(options.fileName ?? '');
  const format = ExpressionParser.parse(options.format ?? false);
  const addBOM = ExpressionParser.parse(options.addBOM ?? false);

  return `
    {
      const source: N8nItem[] = ${source} || inputItems;
      ${ctx.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${ctx.variableName} = [convertItemsToJsonFile(source, {
          binaryProperty: String(${binaryProperty} ?? ''),
          fileName: String(${fileName} ?? ''),
          format: Boolean(${format}),
          addBOM: Boolean(${addBOM}),
        })];
      }
    }
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
}
