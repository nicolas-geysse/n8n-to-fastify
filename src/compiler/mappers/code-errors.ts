import type { N8nNode } from '../core-parser';

export function continuesCodeOnRegularOutput(node: N8nNode): boolean {
  const params = node.parameters ?? {};
  return node.type === 'n8n-nodes-base.code' && node.onError === 'continueRegularOutput' &&
    (params.language ?? 'javaScript') === 'javaScript' &&
    (params.mode ?? 'runOnceForAllItems') === 'runOnceForAllItems';
}
