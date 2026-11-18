import { createBinaryData } from './binary-data';
import type { N8nItem } from './n8n-helper';

interface JsonFileOptions {
  binaryProperty: string;
  fileName: string;
  format: boolean;
  addBOM: boolean;
}

export function convertItemsToJsonFile(items: N8nItem[], options: JsonFileOptions): N8nItem {
  const text = JSON.stringify(items.map(item => item.json), undefined, options.format ? 2 : undefined);
  const bytes = Buffer.from((options.addBOM ? '\uFEFF' : '') + text, 'utf8');
  const file = createBinaryData(bytes, options.fileName || 'file.json', 'application/json');
  return {
    json: {},
    binary: { [options.binaryProperty]: file },
    pairedItem: items.map((_, item) => ({ item })),
  };
}
