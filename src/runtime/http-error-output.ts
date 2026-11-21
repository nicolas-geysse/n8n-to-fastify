import type { N8nItem } from './n8n-helper';

export function partitionHttpErrorItems(items: N8nItem[], inputs: N8nItem[]): [N8nItem[], N8nItem[]] {
  const success: N8nItem[] = [];
  const failure: N8nItem[] = [];
  for (const [index, item] of items.entries()) {
    const keys = Object.keys(item.json).length;
    // n8n also routes response bodies containing only error or error and message.
    if (item.json.error && (keys === 1 || (keys === 2 && item.json.message))) {
      const { json } = inputs[index];
      failure.push({ ...item, json: { ...json, ...item.json } });
    } else {
      success.push(item);
    }
  }
  return [success, failure];
}
