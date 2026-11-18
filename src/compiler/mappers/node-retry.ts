import type { N8nNode } from '../core-parser';

export function usesNodeRetry(node: N8nNode): boolean {
  return node.retryOnFail === true &&
    (node.type === 'n8n-nodes-base.httpRequest' || node.type === 'n8n-nodes-base.s3');
}

export function withNodeRetry(node: N8nNode, operation: string): string {
  if (!usesNodeRetry(node)) return operation;
  const maxTries = Math.min(5, Math.max(2, Math.trunc(Number(node.maxTries) || 3)));
  const delay = Math.min(5000, Math.max(0, Math.trunc(Number(node.waitBetweenTries) || 1000)));
  return `runWithNodeRetry(() => ${operation}, ${maxTries}, ${delay})`;
}
