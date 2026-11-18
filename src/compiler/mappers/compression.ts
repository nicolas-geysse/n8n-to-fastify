import type { N8nNode } from '../core-parser';
import { ExpressionParser } from '../expression-parser';
import type { NodeMapperContext } from './index';

export const compressionNodeType = 'n8n-nodes-base.compression';

export function getCompressionIssue(node: N8nNode): string | undefined {
  const version = node.typeVersion ?? 1;
  if ((version !== 1 && version !== 1.1) ||
    (node.parameters?.operation ?? 'decompress') !== 'decompress') {
    return 'Compression supports ZIP and GZIP decompression on node versions 1 and 1.1';
  }
}

export function mapCompression(ctx: NodeMapperContext): string {
  const issue = getCompressionIssue(ctx.node);
  if (issue) throw new Error(`Node ${JSON.stringify(ctx.node.name)}: ${issue}`);

  const params = ctx.node.parameters ?? {};
  const input = ctx.previousVariableName ?? 'inputItems';
  const fields = ExpressionParser.parse(params.binaryPropertyName ?? 'data');
  const prefix = ExpressionParser.parse(params.outputPrefix ?? 'file_');
  return `
    {
      const source: N8nItem[] = ${input} || inputItems;
      ${ctx.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${ctx.variableName} = await decompressItems(source, String(${fields}), String(${prefix}));
      }
    }
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
}
