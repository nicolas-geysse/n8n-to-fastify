import type { N8nItem } from './n8n-helper';

interface ClashHandling {
  resolveClash?: string;
  mergeMode?: 'deepMerge' | 'shallowMerge';
  overrideEmpty?: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function mergeValue(previous: unknown, incoming: unknown, deep: boolean, overrideEmpty: boolean): unknown {
  if (overrideEmpty && previous !== undefined && (incoming === undefined || incoming === null || incoming === '')) {
    return previous;
  }
  if (!deep) return incoming;
  if (incoming === undefined) return previous;
  if (Array.isArray(incoming)) {
    const merged = Array.isArray(previous) ? [...previous] : [];
    incoming.forEach((value, index) => {
      merged[index] = mergeValue(merged[index], value, true, overrideEmpty);
    });
    return merged;
  }
  if (isRecord(incoming)) return mergeEntries(isRecord(previous) ? previous : {}, incoming, true, overrideEmpty);
  return incoming;
}

function mergeEntries(
  previous: Record<string, unknown>,
  incoming: Record<string, unknown>,
  deep: boolean,
  overrideEmpty: boolean,
): Record<string, unknown> {
  return {
    ...previous,
    ...Object.fromEntries(Object.entries(incoming).map(([key, value]) => [
      key,
      mergeValue(Object.hasOwn(previous, key) ? previous[key] : undefined, value, deep, overrideEmpty),
    ])),
  };
}

export function mergeByPosition(
  inputs: N8nItem[][],
  includeUnpaired: boolean,
  clash: ClashHandling,
): N8nItem[] {
  const lengths = inputs.map(items => items.length);
  const length = includeUnpaired ? Math.max(...lengths) : Math.min(...lengths);
  const preferredMatch = /^preferInput(\d+)$/.exec(clash.resolveClash ?? '');
  const preferredIndex = preferredMatch ? Number(preferredMatch[1]) - 1 : inputs.length - 1;
  const addSuffix = clash.resolveClash === 'addSuffix';
  const portOrder = inputs.map((_, index) => index);
  if (preferredIndex >= 0 && preferredIndex < inputs.length - 1) portOrder.push(preferredIndex);

  return Array.from({ length }, (_, itemIndex) => {
    let json = {};
    let binary: N8nItem['binary'];
    for (const port of portOrder) {
      const item = inputs[port][itemIndex];
      if (!item) continue;
      const source = addSuffix
        ? Object.fromEntries(Object.entries(item.json).map(([key, value]) => [`${key}_${port + 1}`, value]))
        : item.json;
      json = mergeEntries(json, source, clash.mergeMode !== 'shallowMerge', clash.overrideEmpty === true);
      if (item.binary) binary = mergeEntries(binary ?? {}, item.binary, true, false);
    }
    return binary ? { json, binary } : { json };
  });
}
