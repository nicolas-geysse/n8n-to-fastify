import type { N8nItem } from './n8n-helper';

export function extractFromFileItem(
  item: N8nItem,
  operation: string,
  binaryPropertyName: string,
  destinationKey: string,
): N8nItem {
  const field = item.binary?.[binaryPropertyName];
  if (!field || typeof field.data !== 'string') {
    throw new Error(`Extract From File missing binary property "${binaryPropertyName}"`);
  }

  if (operation === 'binaryToPropery' || operation === 'binaryToProperty') {
    return {
      json: { ...item.json, [destinationKey]: field.data },
      binary: item.binary,
      pairedItem: item.pairedItem,
    };
  }

  const decoded = Buffer.from(field.data, 'base64').toString('utf-8');
  if (operation === 'fromJson') {
    const parsed = JSON.parse(decoded);
    return {
      json: typeof parsed === 'object' && parsed !== null ? parsed : { [destinationKey]: parsed },
      binary: item.binary,
      pairedItem: item.pairedItem,
    };
  }

  return {
    json: { ...item.json, [destinationKey]: decoded },
    binary: item.binary,
    pairedItem: item.pairedItem,
  };
}

export async function sendEmailItem(options: {
  from?: string;
  to: string;
  subject: string;
  text?: string;
  html?: string;
}): Promise<N8nItem> {
  const webhookUrl = process.env.EMAIL_WEBHOOK_URL;
  const resendKey = process.env.RESEND_API_KEY;

  if (webhookUrl) {
    const res = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(options),
    });
    if (!res.ok) throw new Error(`Email webhook failed with status ${res.status}`);
    return { json: { accepted: true, to: options.to, subject: options.subject } };
  }

  if (resendKey) {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${resendKey}`,
      },
      body: JSON.stringify({
        from: options.from || process.env.EMAIL_FROM || 'noreply@localhost',
        to: options.to.split(',').map(s => s.trim()).filter(Boolean),
        subject: options.subject,
        ...(options.html ? { html: options.html } : { text: options.text || '' }),
      }),
    });
    if (!res.ok) throw new Error(`Resend API failed with status ${res.status}`);
    const data: any = await res.json();
    return { json: { id: data?.id, accepted: true, to: options.to } };
  }

  throw new Error('EMAIL_WEBHOOK_URL or RESEND_API_KEY environment variable is required for Email Send node');
}

export async function runGoogleCalendar(options: {
  operation: string;
  calendarId: string;
  eventId?: string;
  summary?: string;
  start?: string;
  end?: string;
  description?: string;
}): Promise<N8nItem> {
  const token = process.env.GOOGLE_CALENDAR_ACCESS_TOKEN;
  if (!token) {
    throw new Error('GOOGLE_CALENDAR_ACCESS_TOKEN environment variable is required for Google Calendar node');
  }
  const baseUrl = (process.env.GOOGLE_CALENDAR_BASE_URL || 'https://www.googleapis.com/calendar/v3').replace(/\/+$/, '');
  const cal = encodeURIComponent(options.calendarId || 'primary');

  let url = `${baseUrl}/calendars/${cal}/events`;
  let method = 'GET';
  let body: string | undefined;

  if (options.operation === 'create') {
    method = 'POST';
    body = JSON.stringify({
      summary: options.summary || 'Event',
      ...(options.description ? { description: options.description } : {}),
      ...(options.start ? { start: { dateTime: options.start } } : {}),
      ...(options.end ? { end: { dateTime: options.end } } : {}),
    });
  } else if (options.operation === 'delete' && options.eventId) {
    method = 'DELETE';
    url = `${url}/${encodeURIComponent(options.eventId)}`;
  } else if (options.operation === 'get' && options.eventId) {
    url = `${url}/${encodeURIComponent(options.eventId)}`;
  }

  const res = await fetch(url, {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    ...(body ? { body } : {}),
  });
  if (!res.ok) throw new Error(`Google Calendar API failed with status ${res.status}`);
  if (method === 'DELETE') return { json: { deleted: true, eventId: options.eventId } };
  const data: any = await res.json();
  return { json: data && typeof data === 'object' ? data : { data } };
}

export async function runEvolutionApi(options: {
  resource?: string;
  operation?: string;
  instanceName?: string;
  remoteJid?: string;
  messageText?: string;
  mediaUrl?: string;
  messageId?: string;
}): Promise<N8nItem> {
  const baseUrl = (process.env.EVOLUTION_API_URL || '').replace(/\/+$/, '');
  const apiKey = process.env.EVOLUTION_API_KEY || '';
  if (!baseUrl || !apiKey) {
    throw new Error('EVOLUTION_API_URL and EVOLUTION_API_KEY environment variables are required for Evolution API node');
  }

  const instance = encodeURIComponent(options.instanceName || process.env.EVOLUTION_INSTANCE || 'default');
  const op = (options.operation || 'sendText').toLowerCase();
  let endpoint = `${baseUrl}/message/sendText/${instance}`;
  let method = 'POST';
  let payload: Record<string, unknown> = {
    number: options.remoteJid || '',
    text: options.messageText || '',
  };

  if (op.includes('media') || op.includes('image')) {
    endpoint = `${baseUrl}/message/sendMedia/${instance}`;
    payload = { number: options.remoteJid || '', media: options.mediaUrl || '', caption: options.messageText || '' };
  } else if (op.includes('base64') || op.includes('mediabase64')) {
    endpoint = `${baseUrl}/chat/getBase64FromMediaMessage/${instance}`;
    payload = { message: { key: { id: options.messageId || '' } } };
  }

  const res = await fetch(endpoint, {
    method,
    headers: {
      'Content-Type': 'application/json',
      apikey: apiKey,
    },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`Evolution API failed with status ${res.status}`);
  const data: any = await res.json();
  return { json: data && typeof data === 'object' ? data : { data } };
}
