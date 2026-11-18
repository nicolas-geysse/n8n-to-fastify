import { posix } from 'node:path';
import { promisify } from 'node:util';
import { gunzip as zlibGunzip } from 'node:zlib';
import { unzip, type Unzipped } from 'fflate';
import { createBinaryData } from './binary-data';
import type { N8nItem } from './n8n-helper';

const gunzipBytes = promisify(zlibGunzip);

const DEFAULT_MAX_TOTAL_BYTES = 50 * 1024 * 1024; // 50 MB
const DEFAULT_MAX_ENTRY_BYTES = 25 * 1024 * 1024; // 25 MB
const DEFAULT_MAX_ZIP_ENTRIES = 1000;
const MAX_COMPRESSION_RATIO = 100;
const RATIO_CHECK_MIN_BYTES = 1024 * 1024; // 1 MB

function readPositiveLimit(envName: string, fallback: number): number {
  const raw = process.env[envName];
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function assertSafeArchivePath(filePath: string): void {
  if (
    !filePath ||
    filePath.includes('\0') ||
    filePath.startsWith('/') ||
    filePath.startsWith('\\') ||
    /^[a-zA-Z]:/.test(filePath)
  ) {
    throw new Error('Compression rejected unsafe archive entry path');
  }
  const segments = filePath.split(/[\\/]/);
  if (segments.includes('..')) {
    throw new Error('Compression rejected archive path traversal');
  }
}

function assertCompressionRatio(compressedBytes: number, uncompressedBytes: number): void {
  if (uncompressedBytes <= RATIO_CHECK_MIN_BYTES) return;
  if (compressedBytes <= 0 || uncompressedBytes / compressedBytes > MAX_COMPRESSION_RATIO) {
    throw new Error('Compression rejected archive exceeding maximum compression ratio');
  }
}

async function unzipSafely(bytes: Uint8Array): Promise<Unzipped> {
  const maxTotalBytes = readPositiveLimit('N8N_TO_FASTIFY_MAX_UNZIP_BYTES', DEFAULT_MAX_TOTAL_BYTES);
  const maxEntryBytes = Math.min(DEFAULT_MAX_ENTRY_BYTES, maxTotalBytes);
  const maxEntries = readPositiveLimit('N8N_TO_FASTIFY_MAX_ZIP_ENTRIES', DEFAULT_MAX_ZIP_ENTRIES);

  let entryCount = 0;
  let expectedTotalBytes = 0;
  let filterError: Error | undefined;

  const entries = await new Promise<Unzipped>((resolve, reject) => {
    unzip(
      bytes,
      {
        filter(file) {
          if (file.name.includes('__MACOSX') || file.name.endsWith('/')) return false;
          try {
            assertSafeArchivePath(file.name);
            entryCount += 1;
            if (entryCount > maxEntries) {
              throw new Error('Compression rejected ZIP archive exceeding maximum entry count');
            }
            if (file.originalSize > maxEntryBytes) {
              throw new Error('Compression rejected ZIP entry exceeding maximum uncompressed size');
            }
            expectedTotalBytes += file.originalSize;
            if (expectedTotalBytes > maxTotalBytes) {
              throw new Error('Compression rejected ZIP archive exceeding maximum total uncompressed size');
            }
            assertCompressionRatio(file.size, file.originalSize);
            return true;
          } catch (err) {
            filterError = err instanceof Error ? err : new Error(String(err));
            return false;
          }
        },
      },
      (err, result) => {
        if (filterError) return reject(filterError);
        if (err) return reject(err);
        resolve(result);
      },
    );
  });

  // Re-verify actual decompressed sizes in case the ZIP header lied about originalSize.
  let actualTotalBytes = 0;
  for (const [filePath, fileBytes] of Object.entries(entries)) {
    assertSafeArchivePath(filePath);
    if (fileBytes.byteLength > maxEntryBytes) {
      throw new Error('Compression rejected ZIP entry exceeding maximum uncompressed size');
    }
    actualTotalBytes += fileBytes.byteLength;
    if (actualTotalBytes > maxTotalBytes) {
      throw new Error('Compression rejected ZIP archive exceeding maximum total uncompressed size');
    }
  }
  assertCompressionRatio(bytes.byteLength, actualTotalBytes);
  return entries;
}

export async function decompressItems(items: N8nItem[], fields: string, prefix: string): Promise<N8nItem[]> {
  const maxTotalBytes = readPositiveLimit('N8N_TO_FASTIFY_MAX_UNZIP_BYTES', DEFAULT_MAX_TOTAL_BYTES);
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
        const entries = await unzipSafely(bytes);
        for (const [filePath, fileBytes] of Object.entries(entries)) {
          if (filePath.includes('__MACOSX') || filePath.endsWith('/')) continue;
          const file = createBinaryData(fileBytes, posix.basename(filePath));
          const directory = posix.dirname(filePath);
          binary[prefix + zipIndex++] = directory === '.' ? file : { ...file, directory };
        }
      } else if (extension === 'gz' || extension === 'gzip') {
        const contents = await gunzipBytes(bytes, { maxOutputLength: maxTotalBytes });
        assertCompressionRatio(bytes.byteLength, contents.byteLength);
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
