import { posix } from 'node:path';
import { promisify } from 'node:util';
import { gunzip, unzip } from 'fflate';
import { createBinaryData } from './binary-data';
import type { N8nItem } from './n8n-helper';

const gunzipBytes = promisify(gunzip);
const unzipEntries = promisify(unzip);

export async function decompressItems(items: N8nItem[], fields: string, prefix: string): Promise<N8nItem[]> {
  const fieldNames = fields.split(',').map(field => field.trim());
  const output: N8nItem[] = [];
  for (const [itemIndex, item] of items.entries()) {
    const binary: NonNullable<N8nItem['binary']> = {};
    let zipIndex = 0;
    for (const [fieldIndex, field] of fieldNames.entries()) {
      const archive = item.binary?.[field];
      if (!archive) throw new Error('Compression input is missing the configured binary field');
      if (archive.id || typeof archive.data !== 'string' ||
        archive.data.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(archive.data)) {
        throw new Error('Compression requires inline base64 binary data');
      }
      const bytes = Buffer.from(archive.data, 'base64');
      const extension = typeof archive.fileExtension === 'string' ? archive.fileExtension.toLowerCase() : '';
      if (extension === 'zip') {
        const entries = await unzipEntries(bytes);
        for (const [filePath, fileBytes] of Object.entries(entries)) {
          if (filePath.includes('__MACOSX') || filePath.endsWith('/')) continue;
          const file = createBinaryData(fileBytes, posix.basename(filePath));
          const directory = posix.dirname(filePath);
          binary[prefix + zipIndex++] = directory === '.' ? file : { ...file, directory };
        }
      } else if (extension === 'gz' || extension === 'gzip') {
        const contents = await gunzipBytes(bytes);
        const archiveName = typeof archive.fileName === 'string' ? posix.basename(archive.fileName) : 'file';
        const originalExtension = archiveName.endsWith('.gz') ? posix.extname(archiveName.slice(0, -3)) : '';
        const fileName = (archiveName.split('.')[0] || 'file') + (originalExtension || '.txt');
        binary[prefix + fieldIndex] = createBinaryData(contents, fileName);
      } else {
        throw new Error('Compression requires a ZIP or GZIP file extension');
      }
    }
    output.push({ json: item.json, binary, pairedItem: { item: itemIndex } });
  }
  return output;
}
