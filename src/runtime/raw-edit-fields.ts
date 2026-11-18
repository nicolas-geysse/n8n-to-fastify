import { n8n, type N8nItem } from './n8n-helper';

interface EditFieldsOptions {
  includeAll: boolean;
  stripBinary: boolean;
  dotNotation: boolean;
}

export function encodeJsonTemplateValue(value: unknown, quoted: boolean): string {
  if (!quoted) return JSON.stringify(value) ?? 'null';
  const text = value !== null && typeof value === 'object' ? JSON.stringify(value) : String(value);
  return JSON.stringify(text).slice(1, -1);
}

export function applyRawEditFields(
  input: N8nItem,
  itemIndex: number,
  output: unknown,
  options: EditFieldsOptions,
): N8nItem {
  let fields = output;
  if (typeof fields === 'string') {
    try {
      fields = JSON.parse(fields);
    } catch {
      throw new Error('Edit Fields JSON Output contains invalid JSON at item ' + itemIndex);
    }
  }
  if (fields === null || typeof fields !== 'object' || Array.isArray(fields)) {
    throw new Error('Edit Fields JSON Output expects an object at item ' + itemIndex);
  }

  const json = options.includeAll ? structuredClone(input.json) : {};
  for (const [key, value] of Object.entries(fields)) {
    if (options.dotNotation && key.includes('.')) {
      if (key.split('.').some(part => ['__proto__', 'constructor', 'prototype'].includes(part))) {
        throw new Error('Edit Fields JSON Output contains an invalid field path');
      }
      n8n.setNestedProperty(json, key, value);
    } else {
      Object.defineProperty(json, key, { value, writable: true, configurable: true, enumerable: true });
    }
  }

  return {
    json,
    ...(options.includeAll && !options.stripBinary && input.binary !== undefined
      ? { binary: { ...input.binary } } : {}),
    pairedItem: { item: itemIndex },
  };
}
