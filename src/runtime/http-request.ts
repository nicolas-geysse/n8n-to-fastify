import type { N8nItem } from './n8n-helper';
import { createBinaryData } from './binary-data';

type RequestFormPart = { name: string } & ({ value: string } | { binaryProperty: string });

export function parseRequestHeaders(input: unknown): Headers {
  try {
    const values: unknown = typeof input === 'string' ? JSON.parse(input) : input;
    if (!values || typeof values !== 'object' || Array.isArray(values)) {
      throw new Error('expected object');
    }
    const headers = new Headers();
    for (const [name, value] of Object.entries(values)) {
      const entries: unknown[] = Array.isArray(value) ? value : [value];
      for (const entry of entries) {
        if (typeof entry !== 'string' && typeof entry !== 'number' && typeof entry !== 'boolean') {
          throw new Error('expected scalar value');
        }
        headers.append(name, String(entry));
      }
    }
    return headers;
  } catch {
    throw new Error('HTTP Request JSON headers must be an object with valid header names and values');
  }
}

export function createRequestForm(item: N8nItem, parts: RequestFormPart[]): FormData {
  const form = new FormData();
  for (const part of parts) {
    if (!part.name) throw new Error('HTTP Request multipart fields require a name');
    if ('value' in part) {
      form.append(part.name, part.value);
      continue;
    }

    const binary = item.binary?.[part.binaryProperty];
    if (!binary) throw new Error('HTTP Request multipart input is missing the configured binary field');
    if (binary.id) {
      throw new Error('HTTP Request multipart requires inline binary data rather than a storage reference');
    }
    const { data, fileName, mimeType } = binary;
    if (typeof data !== 'string' || data.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(data)) {
      throw new Error('HTTP Request multipart requires inline base64 binary data');
    }
    const blob = new Blob([Buffer.from(data, 'base64')], {
      type: typeof mimeType === 'string' && mimeType ? mimeType : 'application/octet-stream',
    });
    form.append(part.name, blob, typeof fileName === 'string' && fileName ? fileName : 'file');
  }
  return form;
}

export async function readFileResponse(
  response: Response,
  requestUrl: string,
  item: N8nItem,
  outputProperty: string,
  index: number,
  includeResponseMetadata = false,
): Promise<N8nItem> {
  const bytes = new Uint8Array(await response.arrayBuffer());
  const mimeType = response.headers.get('content-type')?.split(';', 1)[0].trim() || 'application/octet-stream';
  const binary = createBinaryData(bytes, '', mimeType);
  const extension = binary.fileExtension;
  const fileName = extension && requestUrl.endsWith('.' + extension)
    ? requestUrl.split('/').pop()
    : extension ? `${outputProperty}.${extension}` : undefined;

  let json = item.json;
  if (includeResponseMetadata) {
    const headers = Object.fromEntries<string | string[]>(response.headers);
    const cookies = response.headers.getSetCookie();
    if (cookies.length > 0) headers['set-cookie'] = cookies;
    json = { headers, statusCode: response.status, statusMessage: response.statusText };
  }

  return {
    json,
    binary: { ...item.binary, [outputProperty]: { ...binary, fileName } },
    pairedItem: { item: index },
  };
}
