import type { N8nItem } from './n8n-helper';

export interface CommunityNodeExecution {
  node: { id: string; name: string; type: string; typeVersion: number; retryOnFail: boolean; maxTries?: number; waitBetweenTries?: number };
  items: N8nItem[];
  parameters(index?: number): Record<string, unknown>;
}

type Execute = (context: CommunityNodeExecution) => unknown;

function isItemBatch(items: unknown): items is N8nItem[] {
  return Array.isArray(items) && items.every(item => item?.json && typeof item.json === 'object');
}

export async function executeCommunityAdapter(execute: Execute, context: CommunityNodeExecution): Promise<N8nItem[]> {
  const items = await execute(context);
  if (!isItemBatch(items)) {
    throw new Error('Community adapter must return an array of n8n items');
  }
  return items;
}

export async function executeCommunityAdapterPorts(
  execute: Execute,
  context: CommunityNodeExecution,
  outputCount: number,
): Promise<N8nItem[][]> {
  const ports = await execute(context);
  if (!Array.isArray(ports) || ports.length !== outputCount || !Array.from(ports).every(isItemBatch)) {
    throw new Error(`Community adapter must return ${outputCount} arrays of n8n items, one per output port`);
  }
  return ports;
}
