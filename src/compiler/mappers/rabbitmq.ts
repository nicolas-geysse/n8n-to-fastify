import { ExpressionParser } from '../expression-parser';
import type { N8nNode } from '../core-parser';
import type { NodeMapperContext } from './index';

export const rabbitMqPublisherType = 'n8n-nodes-base.rabbitmq';

export function getRabbitMqPublisherIssue(node: N8nNode): string | undefined {
  const params = node.parameters ?? {};
  const version = node.typeVersion ?? 1;
  if (version !== 1 && version !== 1.1) {
    return 'RabbitMQ needs typeVersion 1 or 1.1';
  }
  if ((params.operation ?? 'sendMessage') !== 'sendMessage' || params.mode !== 'exchange' ||
      Object.keys(params.options ?? {}).length > 0) {
    return 'RabbitMQ only supports Send Message to an existing exchange with default options';
  }
}

export function mapRabbitMqPublisher(ctx: NodeMapperContext): string {
  const issue = getRabbitMqPublisherIssue(ctx.node);
  if (issue) throw new Error(`Node ${JSON.stringify(ctx.node.name)}: ${issue}`);

  const params = ctx.node.parameters ?? {};
  const source = ctx.previousVariableName ?? 'inputItems';
  const exchange = ExpressionParser.parse(params.exchange ?? '');
  const routingKey = ExpressionParser.parse(params.routingKey ?? '');
  const sendInputData = ExpressionParser.parse(params.sendInputData ?? true);
  const message = ExpressionParser.parse(params.message ?? '');
  const output = ctx.variableName;

  return `
    {
      const source: N8nItem[] = ${source} || inputItems;
      ${output} = [];
      if (source.length > 0) {
        item = source[0];
        const exchange = String(${exchange});
        const routingKey = String(${routingKey});
        const sendInputData = Boolean(${sendInputData});
        const messages = source.map((item) => sendInputData ? JSON.stringify(item.json) : String(${message}));
        ${output} = await publishRabbitMq(exchange, routingKey, messages);
      }
      item = ${output}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${output});
    }`;
}
