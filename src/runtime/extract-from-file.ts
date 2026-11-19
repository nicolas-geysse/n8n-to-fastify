import { n8n, type N8nItem } from './n8n-helper';

interface ExtractOptions {
  binaryProperty: string;
  destination: string;
  keepSource?: string;
}

export function extractBase64Items(items: N8nItem[], getOptions: (item: N8nItem) => ExtractOptions): N8nItem[] {
  const output: N8nItem[] = [];
  for (const [index, item] of items.entries()) {
    const { binaryProperty, destination, keepSource } = getOptions(item);
    if (!item.binary || !Object.hasOwn(item.binary, binaryProperty) || !item.binary[binaryProperty]) continue;
    const file = item.binary[binaryProperty];
    if (file.id || typeof file.data !== 'string') {
      throw new Error('Extract From File requires inline base64 binary data');
    }

    const preserveJson = keepSource === 'json' || keepSource === 'both';
    const preserveBinary = keepSource === 'binary' || keepSource === 'both';
    const json = preserveJson ? structuredClone(item.json) : {};
    const binary = preserveBinary ? item.binary : structuredClone(item.binary);
    if (!preserveBinary) delete binary[binaryProperty];

    const literal = Object.hasOwn(json, destination);
    const path = literal ? [destination] : destination.split('.');
    if (path.some(part => ['__proto__', 'constructor', 'prototype'].includes(part)) ||
        (!literal && /[\[\]]/.test(destination))) {
      throw new Error('Extract From File requires a safe destination key or dot-separated path');
    }
    const value = Buffer.from(file.data, 'base64').toString('base64');
    if (literal) json[destination] = value;
    else n8n.setNestedProperty(json, destination, value);
    output.push({ json, binary, pairedItem: { item: index } });
  }
  return output;
}
