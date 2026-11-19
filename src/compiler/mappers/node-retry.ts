import type { N8nNode } from '../core-parser';

export function usesNodeRetry(node: N8nNode, linkedChain = false): boolean {
  const code = node.type === 'n8n-nodes-base.code' && node.typeVersion === 2 &&
    (node.parameters?.language ?? 'javaScript') === 'javaScript' &&
    (node.parameters?.mode ?? 'runOnceForAllItems') === 'runOnceForAllItems';
  return node.retryOnFail === true &&
    (code || linkedChain || node.type === 'n8n-nodes-base.httpRequest' || node.type === 'n8n-nodes-base.s3');
}

export function withNodeRetry(node: N8nNode, operation: string, linkedChain = false): string {
  if (!usesNodeRetry(node, linkedChain)) return operation;
  const maxTries = Math.min(5, Math.max(2, Math.trunc(Number(node.maxTries) || 3)));
  const configuredDelay: unknown = node.waitBetweenTries;
  const delayNumber = typeof configuredDelay === 'string' && configuredDelay.trim()
    ? Number(configuredDelay)
    : configuredDelay;
  // Zero is an explicit request to retry without waiting.
  const delay = typeof delayNumber === 'number' && Number.isFinite(delayNumber)
    ? Math.min(5000, Math.max(0, Math.trunc(delayNumber)))
    : 1000;
  return `runWithNodeRetry(() => ${operation}, ${maxTries}, ${delay})`;
}
