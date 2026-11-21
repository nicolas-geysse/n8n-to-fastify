import type { N8nItem } from './n8n-helper';

interface BatchOrigin {
  nodeName: string;
  output: N8nItem[];
  inputRows: N8nItem[];
}

interface BatchParents {
  input: N8nItem[];
  rows: Array<number | undefined>;
}

function pairedIndex(value: unknown): number | undefined {
  if (Array.isArray(value)) {
    if (value.length !== 1) return;
    [value] = value;
  }
  if (typeof value === 'number') return value;
  if (value && typeof value === 'object' && 'item' in value && typeof value.item === 'number' &&
      (!('input' in value) || value.input === undefined || value.input === 0) && !('sourceOverwrite' in value)) {
    return value.item;
  }
}

export class ItemMatching {
  private readonly origins = new WeakMap<N8nItem[], BatchOrigin>();
  private readonly parents = new WeakMap<N8nItem[], Map<string, BatchParents | undefined>>();

  public select(nodeName: string, output: N8nItem[]): N8nItem[] {
    // Pairings received by a node refer to its current input batch, not the previous edge.
    const input = output.map((item, index) => ({ ...item, pairedItem: { item: index } }));
    this.origins.set(input, { nodeName, output, inputRows: input.slice() });
    return input;
  }

  public record(nodeName: string, input: N8nItem[], output: N8nItem[]): void {
    const original = this.origins.get(input)?.inputRows;
    if (!original) return;
    let runs = this.parents.get(output);
    if (!runs) {
      runs = new Map();
      this.parents.set(output, runs);
    }
    // A reused output array with two attributions cannot identify a unique run.
    if (runs.has(nodeName) && runs.get(nodeName)?.input !== input) {
      runs.set(nodeName, undefined);
      return;
    }
    const identities = new Map<N8nItem, number | undefined>();
    original.forEach((item, index) => identities.set(item, identities.has(item) ? undefined : index));
    const rows = output.map((item, index) => {
      if (item.pairedItem !== undefined) return pairedIndex(item.pairedItem);
      if (identities.has(item)) return identities.get(item);
      if (original.length === 1) return 0;
      if (original.length === output.length) return index;
    });
    runs.set(nodeName, { input, rows });
  }

  public resolve(nodeName: string, input: N8nItem[], index: number): N8nItem {
    const seen = new Set<N8nItem[]>();
    let batch = input;
    let row: number | undefined = index;
    while (!seen.has(batch) && row !== undefined && Number.isSafeInteger(row) && row >= 0) {
      const origin = this.origins.get(batch);
      if (!origin || row >= origin.output.length) break;
      if (origin.nodeName === nodeName) return origin.output[row];
      const parent = this.parents.get(origin.output)?.get(origin.nodeName);
      if (!parent) break;
      seen.add(batch);
      batch = parent.input;
      row = parent.rows[row];
    }
    throw new Error('Cannot resolve a unique linked item');
  }
}
