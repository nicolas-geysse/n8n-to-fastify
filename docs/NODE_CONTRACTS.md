# Clean-Room Node Contracts & Compatibility Matrix

This document defines the **interoperability specification** and **input/output (`N8nItem[]`) contracts** for every node supported by `n8n-to-fastify`.

## Clean-Room Engineering Policy

1. **Public Interoperability Only**: Node contracts are specified strictly from exported workflow JSON schemas (`type`, `typeVersion`, `parameters`, `connections`) and observable `N8nItem[]` input/output behavior.
2. **Original MIT Implementation**: Every compiler mapper (`src/compiler/`) and runtime primitive (`src/runtime/`) is written from scratch in TypeScript using Node.js/Bun/Fastify built-ins or explicitly audited MIT/Apache-2.0/ISC/BSD dependencies.
3. **Contract-First Verification**: Every supported node type and `typeVersion` must have automated input/output contract tests covering:
   - Normal single-item and multi-item `N8nItem[]` batches
   - Empty input batches (`[]`)
   - Binary/metadata preservation (`binary`, `pairedItem`)
   - Error handling (`--strict` diagnostics, `onError`, `retryOnFail`, fail-fast guards)

---

## Node Contract Matrix

| Node `type` | `typeVersion` | Primary Parameters | I/O Contract | Contract Tests |
| :--- | :--- | :--- | :--- | :--- |
| `n8n-nodes-base.webhook` | 1, 2 | `path`, `httpMethod` | Registers Fastify route; passes body/query/headers as `[{ json }]` | [compiler.test.ts](file:///rogerio/core/n8n-to-fastify/tests/compiler.test.ts), [webhook-error.test.ts](file:///rogerio/core/n8n-to-fastify/tests/webhook-error.test.ts) |
| `n8n-nodes-base.respondToWebhook` | 1 | `respondWith` (`firstIncomingItem`, `allIncomingItems`, `json`, `text`, `noData`), `responseBody`, `options.responseCode` | Sets HTTP response status + payload; passes input batch through | [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.code` / `function` / `functionItem` | 1, 2 | `jsCode`, `functionCode`, `mode`, `onError` | Executes async JS with `$input`, `$json`, `$env`, `$items`, `$('Node').isExecuted`, `$node`; normalizes to `N8nItem[]` | [code-node-items.test.ts](file:///rogerio/core/n8n-to-fastify/tests/code-node-items.test.ts), [code-errors.test.ts](file:///rogerio/core/n8n-to-fastify/tests/code-errors.test.ts), [legacy-items.test.ts](file:///rogerio/core/n8n-to-fastify/tests/legacy-items.test.ts), [legacy-node.test.ts](file:///rogerio/core/n8n-to-fastify/tests/legacy-node.test.ts) |
| `n8n-nodes-base.executeWorkflow` / `executeWorkflowTrigger` | 1 | `workflowId` | Invokes registered subworkflow in-memory via `executeSubworkflow`; propagates `N8nItem[]` | [compiler.test.ts](file:///rogerio/core/n8n-to-fastify/tests/compiler.test.ts), [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.if` | 1, 2.2 | `conditions` | Routes each item to port `0` (true) or port `1` (false) | [branch-outputs.test.ts](file:///rogerio/core/n8n-to-fastify/tests/branch-outputs.test.ts) |
| `n8n-nodes-base.filter` | 1, 2.2 | `conditions` | Keeps matching items in order on port `0` | [branch-outputs.test.ts](file:///rogerio/core/n8n-to-fastify/tests/branch-outputs.test.ts) |
| `n8n-nodes-base.switch` | 3.2, 3.3 | `mode` (`rules`, `expression`), `rules`, `output`, `options.fallbackOutput`, `options.allMatchingOutputs`, `onError` | Routes items per-item to matching output ports | [branch-outputs.test.ts](file:///rogerio/core/n8n-to-fastify/tests/branch-outputs.test.ts) |
| `n8n-nodes-base.set` | 1, 2, 3.4 | `mode`, `assignments`, `values`, `jsonOutput`, `include`, `options.dotNotation`, `keepOnlySet` | Transforms `json` fields, coerces numbers, guards `__proto__`/`constructor`/`prototype`, preserves `binary` | [set-number.test.ts](file:///rogerio/core/n8n-to-fastify/tests/set-number.test.ts), [set-include.test.ts](file:///rogerio/core/n8n-to-fastify/tests/set-include.test.ts), [set-binary.test.ts](file:///rogerio/core/n8n-to-fastify/tests/set-binary.test.ts), [raw-edit-fields.test.ts](file:///rogerio/core/n8n-to-fastify/tests/raw-edit-fields.test.ts), [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.httpRequest` | 1–4.2 | `method`, `url`, `sendHeaders`, `jsonHeaders`, `sendBody`, `contentType`, `jsonBody`, `options.timeout`, `options.response`, `retryOnFail` | Native `fetch` with timeout, retry, status check, multipart `FormData`, file binary output, `$node` bindings | [http-multipart.test.ts](file:///rogerio/core/n8n-to-fastify/tests/http-multipart.test.ts), [http-file-response.test.ts](file:///rogerio/core/n8n-to-fastify/tests/http-file-response.test.ts), [http-json-body.test.ts](file:///rogerio/core/n8n-to-fastify/tests/http-json-body.test.ts), [http-request-status.test.ts](file:///rogerio/core/n8n-to-fastify/tests/http-request-status.test.ts), [http-request-timeout.test.ts](file:///rogerio/core/n8n-to-fastify/tests/http-request-timeout.test.ts), [http-request-retry.test.ts](file:///rogerio/core/n8n-to-fastify/tests/http-request-retry.test.ts) |
| `n8n-nodes-base.aggregate` / `itemLists` | 1 | `destinationFieldName` | Collapses `N8nItem[]` into `[{ json: { [field]: items.map(i => i.json) } }]` | [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.splitOut` | 1 | `fieldToSplitOut` | Expands array field into individual `N8nItem[]` | [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.merge` | 2, 3 | `mode` (`append`, `combine`, `combineByPosition`), `numberInputs`, `options.includeUnpaired`, `options.clashHandling` | Appends or merges up to 10 position ports | [position-merge.test.ts](file:///rogerio/core/n8n-to-fastify/tests/position-merge.test.ts), [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.postgres` | 1, 2 | `query` | Requires `DATABASE_URL`; executes via `POSTGRES_HTTP_PROXY` or emits prepared metadata | [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.postgresTrigger` | 1 | `channelName` | Opens `pg` `LISTEN` session with reconnect and graceful drain | [postgres-trigger.test.ts](file:///rogerio/core/n8n-to-fastify/tests/postgres-trigger.test.ts) |
| `n8n-nodes-base.rabbitmq` | 1, 1.1 | `exchange`, `routingKey` | Publishes 1 JSON message per item via `RABBITMQ_URL` with broker confirmation | [rabbitmq.test.ts](file:///rogerio/core/n8n-to-fastify/tests/rabbitmq.test.ts) |
| `n8n-nodes-base.rabbitmqTrigger` | 1 | `queue`, `options.acknowledge`, `options.parallelMessages` | Consumes queue with immediate/completion/success ack and graceful drain | [rabbitmq-trigger.test.ts](file:///rogerio/core/n8n-to-fastify/tests/rabbitmq-trigger.test.ts) |
| `n8n-nodes-base.s3` | 1 | `bucketName`, `fileKey`, `binaryPropertyName`, `retryOnFail` | Downloads objects in order into inline base64 `binary` items | [s3-download.test.ts](file:///rogerio/core/n8n-to-fastify/tests/s3-download.test.ts) |
| `n8n-nodes-base.convertToFile` | 1.1 | `operation` (`toJson`), `mode` (`once`), `binaryPropertyName` | Serializes `inputItems` to UTF-8 JSON binary item | [convert-to-json.test.ts](file:///rogerio/core/n8n-to-fastify/tests/convert-to-json.test.ts) |
| `n8n-nodes-base.compression` | 1 | `operation` (`decompress`), `binaryPropertyName` | Decompresses ZIP/GZIP with anti-Zip-Bomb & anti-Zip-Slip guards | [compression.test.ts](file:///rogerio/core/n8n-to-fastify/tests/compression.test.ts) |
| `n8n-nodes-base.wait` | 1, 1.1 | `amount`, `unit`, `resume` | In-process delay preserving batch; rejects unsupported `resume`/`unit` in `--strict` | [wait-interval.test.ts](file:///rogerio/core/n8n-to-fastify/tests/wait-interval.test.ts) |
| `n8n-nodes-base.stopAndError` | 1 | `errorType`, `errorMessage`, `errorObject` | Throws `WorkflowStopError` on active branch | [stop-and-error.test.ts](file:///rogerio/core/n8n-to-fastify/tests/stop-and-error.test.ts) |
| `n8n-nodes-base.noOp` | 1 | — | Pass-through preserving full input batch; `--strict` requires `typeVersion` 1 | [noop.test.ts](file:///rogerio/core/n8n-to-fastify/tests/noop.test.ts), [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `@n8n/n8n-nodes-langchain.{openAi,googleGemini,chainLlm}` | 1 | `model`, `modelId`, `prompt`, `text`, `messages` | Calls OpenAI-compatible `/chat/completions`; fails fast on non-2xx | [node-contracts.test.ts](file:///rogerio/core/n8n-to-fastify/tests/node-contracts.test.ts) |
| `n8n-nodes-base.stickyNote` | 1 | — | Ignored cleanly during compilation and `--strict` checks | [strict-cli.test.ts](file:///rogerio/core/n8n-to-fastify/tests/strict-cli.test.ts) |

---

## Adding a New Node (Checklist)

1. Verify the public exported workflow JSON schema (`type`, `typeVersion`, `parameters`, `connections`).
2. Add the node type to `supportedNodeTypes` and add `--strict` validation in `src/compiler/compatibility.ts`.
3. Implement the mapper in `src/compiler/mappers/` (and runtime helper in `src/runtime/` if needed) using native primitives.
4. Add comprehensive I/O contract tests in `tests/` (normal batch, empty batch, binary preservation, error/strict paths).
5. Rebuild the unminified playground (`bun run build:playground`), run `bun x tsc --noEmit && bun test`, and update this matrix + `README.md` + `llms.txt`.
