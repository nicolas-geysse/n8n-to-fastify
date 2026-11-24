export type LlmProvider = 'openai' | 'anthropic' | 'gemini' | 'groq';

export interface LlmCompletionOptions {
  provider: LlmProvider;
  model: string;
  prompt: string;
  systemMessage?: string;
  temperature?: number;
  maxTokens?: number;
}

export interface LlmCompletionResult {
  text: string;
  model: string;
  provider: LlmProvider;
  usage?: unknown;
}

/**
 * Executes a chat completion against OpenAI, Anthropic Claude, Google Gemini, or Groq
 * using native fetch() with zero external SDK dependencies.
 */
export async function runLlmCompletion(options: LlmCompletionOptions): Promise<LlmCompletionResult> {
  const { provider, model, prompt, systemMessage, temperature, maxTokens } = options;

  if (provider === 'anthropic') {
    const apiKey = process.env.ANTHROPIC_API_KEY || process.env.LLM_API_KEY || '';
    const baseUrl = (process.env.ANTHROPIC_BASE_URL || 'https://api.anthropic.com/v1').replace(/\/+$/, '');
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model,
        max_tokens: maxTokens ?? 1024,
        ...(systemMessage ? { system: systemMessage } : {}),
        ...(temperature !== undefined ? { temperature } : {}),
        messages: [{ role: 'user', content: prompt }],
      }),
    });
    if (!res.ok) {
      throw new Error(`LLM Request failed with status ${res.status}`);
    }
    const data: any = await res.json();
    const text = Array.isArray(data?.content)
      ? data.content.filter((b: any) => b?.type === 'text').map((b: any) => b.text).join('')
      : '';
    return { text, model, provider, usage: data?.usage };
  }

  if (provider === 'gemini') {
    const geminiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
    if (geminiKey || !process.env.OPENAI_API_KEY) {
      const apiKey = geminiKey || process.env.LLM_API_KEY || '';
      const baseUrl = (process.env.GEMINI_BASE_URL || 'https://generativelanguage.googleapis.com/v1beta').replace(/\/+$/, '');
      const res = await fetch(`${baseUrl}/models/${encodeURIComponent(model)}:generateContent`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-goog-api-key': apiKey,
        },
        body: JSON.stringify({
          ...(systemMessage ? { systemInstruction: { parts: [{ text: systemMessage }] } } : {}),
          contents: [{ role: 'user', parts: [{ text: prompt }] }],
          ...(temperature !== undefined || maxTokens !== undefined
            ? {
                generationConfig: {
                  ...(temperature !== undefined ? { temperature } : {}),
                  ...(maxTokens !== undefined ? { maxOutputTokens: maxTokens } : {}),
                },
              }
            : {}),
        }),
      });
      if (!res.ok) {
        throw new Error(`LLM Request failed with status ${res.status}`);
      }
      const data: any = await res.json();
      const text = data?.candidates?.[0]?.content?.parts?.map((p: any) => p?.text ?? '').join('') || '';
      return { text, model, provider, usage: data?.usageMetadata };
    }
  }

  // OpenAI, Groq, and OpenAI-compatible endpoints
  const isGroq = provider === 'groq';
  const apiKey = isGroq
    ? (process.env.GROQ_API_KEY || process.env.LLM_API_KEY || '')
    : (process.env.OPENAI_API_KEY || process.env.LLM_API_KEY || '');
  const defaultBase = isGroq ? 'https://api.groq.com/openai/v1' : 'https://api.openai.com/v1';
  const baseUrl = (isGroq ? (process.env.GROQ_BASE_URL || defaultBase) : (process.env.LLM_BASE_URL || defaultBase)).replace(/\/+$/, '');

  const messages: Array<{ role: string; content: string }> = [];
  if (systemMessage) messages.push({ role: 'system', content: systemMessage });
  messages.push({ role: 'user', content: prompt });

  const res = await fetch(`${baseUrl}/chat/completions`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      messages,
      ...(temperature !== undefined ? { temperature } : {}),
      ...(maxTokens !== undefined ? { max_tokens: maxTokens } : {}),
    }),
  });
  if (!res.ok) {
    throw new Error(`LLM Request failed with status ${res.status}`);
  }
  const data: any = await res.json();
  const text = data?.choices?.[0]?.message?.content || '';
  return { text, model, provider, usage: data?.usage };
}
