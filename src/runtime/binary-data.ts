import { createRequire } from 'node:module';
import { posix } from 'node:path';

const require = createRequire(import.meta.url);

function formatSize(byteCount: number): string {
  const units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB'];
  const exponent = byteCount === 0 ? 0 : Math.min(units.length - 1, Math.floor(Math.log10(byteCount) / 3));
  const value = Number((byteCount / 1000 ** exponent).toPrecision(3));
  return `${value} ${units[exponent]}`;
}

function fileTypeFromMimeType(mimeType: string): string | undefined {
  const [category, subtype] = mimeType.toLowerCase().split(';')[0].split('/');
  if (['image', 'audio', 'video'].includes(category)) return category;
  if (category === 'text') return subtype === 'html' ? 'html' : 'text';
  if (category === 'application') {
    if (subtype === 'json' || subtype === 'pdf') return subtype;
    if (subtype === 'javascript') return 'text';
  }
}

export function createBinaryData(bytes: Uint8Array, fileName: string, contentType?: string) {
  const types = require('mime-types') as {
    lookup(path: string): string | false;
    extension(type: string): string | false;
  };
  const mimeType = contentType || types.lookup(fileName) || 'text/plain';
  const fileExtension = posix.extname(fileName).slice(1) || types.extension(mimeType) || undefined;
  return {
    data: Buffer.from(bytes).toString('base64'),
    mimeType,
    fileType: fileTypeFromMimeType(mimeType),
    fileName,
    fileExtension,
    fileSize: formatSize(bytes.byteLength),
  };
}
