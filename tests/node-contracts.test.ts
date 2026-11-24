import { describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { n8n } from '../src/runtime/n8n-helper';
import { WorkflowContext } from '../src/runtime/workflow-context';

const cli = path.resolve('bin/cli.ts');

function compileWorkflow(name: string, workflow: Record<string, unknown>, strict = true) {
  const dir = mkdtempSync(path.join(tmpdir(), `n2f-contract-${name}-`));
  const input = path.join(dir, `${name}.json`);
  const out = path.join(dir, 'service');
  writeFileSync(input, JSON.stringify(workflow));
  const args = [process.execPath, cli, 'compile', input, '--out', out];
  if (strict) args.push('--strict');
  const res = Bun.spawnSync(args);
  return { dir, out, res };
}

describe('Node I/O Contracts', () => {
  test('respondToWebhook handles firstIncomingItem, allIncomingItems, json, text, noData and custom status', async () => {
    const modes = [
      { respondWith: 'firstIncomingItem', status: 200, expected: { a: 1 } },
      { respondWith: 'allIncomingItems', status: 201, expected: [{ a: 1 }, { a: 2 }] },
      { respondWith: 'json', responseBody: '={{ JSON.stringify({ sum: $json.a + 10 }) }}', status: 202, expected: { sum: 11 } },
      { respondWith: 'text', responseBody: 'ok-plain', status: 200, expected: 'ok-plain' },
      { respondWith: 'noData', status: 204, expected: '' },
    ];

    for (const [idx, cfg] of modes.entries()) {
      const { dir, out, res } = compileWorkflow(`rtw_${idx}`, {
        nodes: [{
          name: 'Respond',
          type: 'n8n-nodes-base.respondToWebhook',
          typeVersion: 1,
          parameters: {
            respondWith: cfg.respondWith,
            ...(cfg.responseBody ? { responseBody: cfg.responseBody } : {}),
            options: { responseCode: cfg.status },
          },
        }],
      });
      try {
        expect(res.exitCode).toBe(0);
        const mod = await import(path.join(out, 'workflows', `rtw_${idx}.ts`));
        const run = mod[`workflow_rtw_${idx}`];
        const ctx = new WorkflowContext();
        const result = await run([{ json: { a: 1 } }, { json: { a: 2 } }], ctx);
        expect(result.items).toEqual([{ json: { a: 1 } }, { json: { a: 2 } }]);
        expect(ctx.webhookResponse).toEqual({
          statusCode: cfg.status,
          body: cfg.expected,
          headers: undefined,
        });
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  test('aggregate and itemLists collapse multi-item and empty batches cleanly', async () => {
    for (const [idx, nodeType] of ['n8n-nodes-base.aggregate', 'n8n-nodes-base.itemLists'].entries()) {
      const { dir, out, res } = compileWorkflow(`agg_${idx}`, {
        nodes: [{
          name: 'Collect',
          type: nodeType,
          typeVersion: 1,
          parameters: { destinationFieldName: 'rows' },
        }],
      });
      try {
        expect(res.exitCode).toBe(0);
        const mod = await import(path.join(out, 'workflows', `agg_${idx}.ts`));
        const run = mod[`workflow_agg_${idx}`];

        const multi = await run([{ json: { id: 1 } }, { json: { id: 2 } }]);
        expect(multi.items).toEqual([{ json: { rows: [{ id: 1 }, { id: 2 }] } }]);

        const empty = await run([]);
        expect(empty.items).toEqual([{ json: { rows: [] } }]);
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  test('splitOut expands object arrays and primitive arrays', async () => {
    const { dir, out, res } = compileWorkflow('split', {
      nodes: [{
        name: 'Split',
        type: 'n8n-nodes-base.splitOut',
        typeVersion: 1,
        parameters: { fieldToSplitOut: 'tags' },
      }],
    });
    try {
      expect(res.exitCode).toBe(0);
      const { workflow_split } = await import(path.join(out, 'workflows', 'split.ts'));

      const objects = await workflow_split([{ json: { tags: [{ name: 'a' }, { name: 'b' }] } }]);
      expect(objects.items).toEqual([{ json: { name: 'a' } }, { json: { name: 'b' } }]);

      const primitives = await workflow_split([{ json: { tags: ['x', 'y'] } }]);
      expect(primitives.items).toEqual([{ json: { tags: 'x' } }, { json: { tags: 'y' } }]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('merge append and combine modes preserve multi-branch items', async () => {
    const { dir, out, res } = compileWorkflow('merge_modes', {
      nodes: [
        { name: 'Left', type: 'n8n-nodes-base.code', typeVersion: 2, parameters: { jsCode: 'return [{ json: { a: 1 } }, { json: { a: 2 } }];' } },
        { name: 'Right', type: 'n8n-nodes-base.code', typeVersion: 2, parameters: { jsCode: 'return [{ json: { b: 10 } }];' } },
        { name: 'Combine', type: 'n8n-nodes-base.merge', typeVersion: 2, parameters: { mode: 'combine' } },
      ],
      connections: {
        Left: { main: [[{ node: 'Combine', type: 'main', index: 0 }]] },
        Right: { main: [[{ node: 'Combine', type: 'main', index: 1 }]] },
      },
    });
    try {
      expect(res.exitCode).toBe(0);
      const { workflow_merge_modes } = await import(path.join(out, 'workflows', 'merge_modes.ts'));
      const result = await workflow_merge_modes([{ json: {} }]);
      expect(result.items).toEqual([
        { json: { a: 1, b: 10 } },
        { json: { a: 2 } },
      ]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('n8n.setNestedProperty supports arrays and blocks prototype pollution', () => {
    const target: Record<string, any> = {};
    n8n.setNestedProperty(target, 'rows[0].name', 'first');
    n8n.setNestedProperty(target, 'rows.1.name', 'second');
    n8n.setNestedProperty(target, 'matrix[0][0]', 42);
    n8n.setNestedProperty(target, '__proto__.polluted', true);
    n8n.setNestedProperty(target, 'constructor.prototype.polluted', true);
    n8n.setNestedProperty(target, 'prototype.polluted', true);

    expect(target).toEqual({
      rows: [{ name: 'first' }, { name: 'second' }],
      matrix: [[42]],
    });
    expect(({} as any).polluted).toBeUndefined();
  });

  test('postgres query node requires DATABASE_URL and handles proxy responses', async () => {
    const { dir, out, res } = compileWorkflow('pg_query', {
      nodes: [{
        name: 'PG',
        type: 'n8n-nodes-base.postgres',
        typeVersion: 2,
        parameters: { query: 'SELECT * FROM users WHERE id = {{ $json.id }}' },
      }],
    });
    const priorDb = process.env.DATABASE_URL;
    const priorProxy = process.env.POSTGRES_HTTP_PROXY;
    const originalFetch = globalThis.fetch;
    try {
      expect(res.exitCode).toBe(0);
      const { workflow_pg_query } = await import(path.join(out, 'workflows', 'pg_query.ts'));

      delete process.env.DATABASE_URL;
      await expect(workflow_pg_query([{ json: { id: 7 } }])).rejects.toThrow('DATABASE_URL');

      process.env.DATABASE_URL = 'postgres://localhost:5432/test';
      process.env.POSTGRES_HTTP_PROXY = 'http://pg-proxy.invalid/query';
      globalThis.fetch = (async (_url, init) => {
        const body = JSON.parse(String(init?.body));
        expect(body.query).toContain('7');
        return Response.json([{ id: 7, name: 'Ada' }]);
      }) as typeof fetch;

      const result = await workflow_pg_query([{ json: { id: 7 } }]);
      expect(result.items).toEqual([{ json: { id: 7, name: 'Ada' } }]);
    } finally {
      globalThis.fetch = originalFetch;
      if (priorDb === undefined) delete process.env.DATABASE_URL;
      else process.env.DATABASE_URL = priorDb;
      if (priorProxy === undefined) delete process.env.POSTGRES_HTTP_PROXY;
      else process.env.POSTGRES_HTTP_PROXY = priorProxy;
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('LLM nodes (openAi, googleGemini, chainLlm) extract completions and fail fast on HTTP errors', async () => {
    const originalFetch = globalThis.fetch;
    const priorKey = process.env.OPENAI_API_KEY;
    process.env.OPENAI_API_KEY = 'test-key';
    try {
      for (const [idx, nodeType] of [
        '@n8n/n8n-nodes-langchain.openAi',
        '@n8n/n8n-nodes-langchain.googleGemini',
        '@n8n/n8n-nodes-langchain.chainLlm',
      ].entries()) {
        const { dir, out, res } = compileWorkflow(`llm_${idx}`, {
          nodes: [{
            name: 'LLM',
            type: nodeType,
            typeVersion: 1,
            parameters: { model: 'gpt-4o-mini', prompt: 'Hello {{ $json.user }}' },
          }],
        });
        try {
          expect(res.exitCode).toBe(0);
          const mod = await import(path.join(out, 'workflows', `llm_${idx}.ts`));
          const run = mod[`workflow_llm_${idx}`];

          globalThis.fetch = (async () =>
            Response.json({
              choices: [{ message: { content: 'Hi Ada!' } }],
              usage: { total_tokens: 12 },
            })
          ) as unknown as typeof fetch;

          const ok = await run([{ json: { user: 'Ada' } }]);
          expect(ok.items).toEqual([{
            json: { text: 'Hi Ada!', model: 'gpt-4o-mini', usage: { total_tokens: 12 } },
          }]);

          globalThis.fetch = (async () =>
            new Response('Unauthorized', { status: 401 })
          ) as unknown as typeof fetch;

          await expect(run([{ json: { user: 'Ada' } }])).rejects.toThrow('LLM Request failed with status 401');
        } finally {
          rmSync(dir, { recursive: true, force: true });
        }
      }
    } finally {
      globalThis.fetch = originalFetch;
      if (priorKey === undefined) delete process.env.OPENAI_API_KEY;
      else process.env.OPENAI_API_KEY = priorKey;
    }
  });

  test('Anthropic Claude, Google Gemini native API, and Groq use their native REST endpoints and headers', async () => {
    const originalFetch = globalThis.fetch;
    const priorOpenAi = process.env.OPENAI_API_KEY;
    const priorAnthropic = process.env.ANTHROPIC_API_KEY;
    const priorGemini = process.env.GEMINI_API_KEY;
    const priorGroq = process.env.GROQ_API_KEY;
    delete process.env.OPENAI_API_KEY;
    process.env.ANTHROPIC_API_KEY = 'ant-test-key';
    process.env.GEMINI_API_KEY = 'gem-test-key';
    process.env.GROQ_API_KEY = 'groq-test-key';

    try {
      // 1. Anthropic Claude
      {
        const { dir, out, res } = compileWorkflow('claude_native', {
          nodes: [{
            name: 'Claude',
            type: '@n8n/n8n-nodes-langchain.anthropic',
            typeVersion: 1,
            parameters: {
              model: 'claude-3-5-sonnet-latest',
              prompt: 'Summarize {{ $json.topic }}',
              options: { systemMessage: 'Be concise', temperature: 0.2, maxTokens: 256 },
            },
          }],
        });
        try {
          expect(res.exitCode).toBe(0);
          const { workflow_claude_native } = await import(path.join(out, 'workflows', 'claude_native.ts'));
          globalThis.fetch = (async (url, init) => {
            expect(String(url)).toBe('https://api.anthropic.com/v1/messages');
            const headers = new Headers(init?.headers);
            expect(headers.get('x-api-key')).toBe('ant-test-key');
            expect(headers.get('anthropic-version')).toBe('2023-06-01');
            const body = JSON.parse(String(init?.body));
            expect(body.model).toBe('claude-3-5-sonnet-latest');
            expect(body.system).toBe('Be concise');
            expect(body.max_tokens).toBe(256);
            return Response.json({
              content: [{ type: 'text', text: 'Claude summary' }],
              usage: { input_tokens: 5, output_tokens: 2 },
            });
          }) as unknown as typeof fetch;

          const outItems = await workflow_claude_native([{ json: { topic: 'Fastify' } }]);
          expect(outItems.items).toEqual([{
            json: { text: 'Claude summary', model: 'claude-3-5-sonnet-latest', usage: { input_tokens: 5, output_tokens: 2 } },
          }]);
        } finally {
          rmSync(dir, { recursive: true, force: true });
        }
      }

      // 2. Google Gemini native API
      {
        const { dir, out, res } = compileWorkflow('gemini_native', {
          nodes: [{
            name: 'Gemini',
            type: '@n8n/n8n-nodes-langchain.googleGemini',
            typeVersion: 1,
            parameters: { model: 'gemini-2.5-flash', prompt: 'Hello {{ $json.name }}' },
          }],
        });
        try {
          expect(res.exitCode).toBe(0);
          const { workflow_gemini_native } = await import(path.join(out, 'workflows', 'gemini_native.ts'));
          globalThis.fetch = (async (url, init) => {
            expect(String(url)).toBe('https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent');
            expect(new Headers(init?.headers).get('x-goog-api-key')).toBe('gem-test-key');
            return Response.json({
              candidates: [{ content: { parts: [{ text: 'Gemini reply' }] } }],
              usageMetadata: { totalTokenCount: 9 },
            });
          }) as unknown as typeof fetch;

          const outItems = await workflow_gemini_native([{ json: { name: 'Ada' } }]);
          expect(outItems.items).toEqual([{
            json: { text: 'Gemini reply', model: 'gemini-2.5-flash', usage: { totalTokenCount: 9 } },
          }]);
        } finally {
          rmSync(dir, { recursive: true, force: true });
        }
      }

      // 3. Groq
      {
        const { dir, out, res } = compileWorkflow('groq_native', {
          nodes: [{
            name: 'Groq',
            type: '@n8n/n8n-nodes-langchain.lmChatGroq',
            typeVersion: 1,
            parameters: { model: 'llama-3.3-70b-versatile', prompt: 'Ping {{ $json.id }}' },
          }],
        });
        try {
          expect(res.exitCode).toBe(0);
          const { workflow_groq_native } = await import(path.join(out, 'workflows', 'groq_native.ts'));
          globalThis.fetch = (async (url, init) => {
            expect(String(url)).toBe('https://api.groq.com/openai/v1/chat/completions');
            expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer groq-test-key');
            return Response.json({
              choices: [{ message: { content: 'Groq pong' } }],
              usage: { total_tokens: 7 },
            });
          }) as unknown as typeof fetch;

          const outItems = await workflow_groq_native([{ json: { id: 1 } }]);
          expect(outItems.items).toEqual([{
            json: { text: 'Groq pong', model: 'llama-3.3-70b-versatile', usage: { total_tokens: 7 } },
          }]);
        } finally {
          rmSync(dir, { recursive: true, force: true });
        }
      }
    } finally {
      globalThis.fetch = originalFetch;
      if (priorOpenAi === undefined) delete process.env.OPENAI_API_KEY;
      else process.env.OPENAI_API_KEY = priorOpenAi;
      if (priorAnthropic === undefined) delete process.env.ANTHROPIC_API_KEY;
      else process.env.ANTHROPIC_API_KEY = priorAnthropic;
      if (priorGemini === undefined) delete process.env.GEMINI_API_KEY;
      else process.env.GEMINI_API_KEY = priorGemini;
      if (priorGroq === undefined) delete process.env.GROQ_API_KEY;
      else process.env.GROQ_API_KEY = priorGroq;
    }
  });

  test('strict mode rejects unsupported retryOnFail nodes and non-v1 noOp', () => {
    const { dir, res } = compileWorkflow('strict_contracts', {
      nodes: [
        { name: 'WaitRetry', type: 'n8n-nodes-base.wait', typeVersion: 1, retryOnFail: true, parameters: { amount: 0 } },
        { name: 'NoOpV2', type: 'n8n-nodes-base.noOp', typeVersion: 2, parameters: {} },
      ],
    }, true);
    try {
      expect(res.exitCode).toBe(1);
      const err = res.stderr.toString();
      expect(err).toContain('retryOnFail is only supported for HTTP Request and S3 Download nodes');
      expect(err).toContain('No Operation needs typeVersion 1');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
