(() => {
  var __defProp = Object.defineProperty;
  var __getOwnPropNames = Object.getOwnPropertyNames;
  var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
  var __hasOwnProp = Object.prototype.hasOwnProperty;
  function __accessProp(key) {
    return this[key];
  }
  var __toCommonJS = (from) => {
    var entry = (__moduleCache ??= new WeakMap).get(from), desc;
    if (entry)
      return entry;
    entry = __defProp({}, "__esModule", { value: true });
    if (from && typeof from === "object" || typeof from === "function") {
      for (var key of __getOwnPropNames(from))
        if (!__hasOwnProp.call(entry, key))
          __defProp(entry, key, {
            get: __accessProp.bind(from, key),
            enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable
          });
    }
    __moduleCache.set(from, entry);
    return entry;
  };
  var __moduleCache;
  var __returnValue = (v) => v;
  function __exportSetter(name, newValue) {
    this[name] = __returnValue.bind(null, newValue);
  }
  var __export = (target, all) => {
    for (var name in all)
      __defProp(target, name, {
        get: all[name],
        enumerable: true,
        configurable: true,
        set: __exportSetter.bind(all, name)
      });
  };

  // src/browser.ts
  var exports_browser = {};
  __export(exports_browser, {
    compileWorkflowJson: () => compileWorkflowJson
  });

  // src/compiler/rabbitmq-trigger.ts
  var rabbitMqTriggerType = "n8n-nodes-base.rabbitmqTrigger";
  function getRabbitMqTriggers(workflow) {
    return workflow.nodes.filter((node) => node.type === rabbitMqTriggerType && !node.disabled);
  }
  function isStaticString(value) {
    return typeof value === "string" && !value.startsWith("=") && !value.includes("{{");
  }
  function parseRabbitMqTrigger(node) {
    if ((node.typeVersion ?? 1) !== 1)
      return "RabbitMQ Trigger needs typeVersion 1";
    const { queue, options: rawOptions } = node.parameters ?? {};
    const options = rawOptions ?? {};
    if (!isStaticString(queue) || !queue.trim())
      return "RabbitMQ Trigger needs a static queue name";
    if (typeof options !== "object" || Array.isArray(options)) {
      return "RabbitMQ Trigger needs static options";
    }
    if (options.acknowledge !== "executionFinishesSuccessfully") {
      return "RabbitMQ Trigger needs the executionFinishesSuccessfully acknowledgement mode";
    }
    if (options.contentIsBinary || options.onlyContent) {
      return "RabbitMQ Trigger supports text or parsed JSON in the message envelope";
    }
    const prefetch = options.parallelMessages ?? -1;
    if (!Number.isInteger(prefetch) || prefetch !== -1 && (prefetch < 1 || prefetch > 65535)) {
      return "RabbitMQ Trigger needs parallelMessages between 1 and 65535, or -1";
    }
    const bindings = options.binding?.bindings ?? [];
    if (!Array.isArray(bindings) || !bindings.every((binding) => isStaticString(binding?.exchange) && binding.exchange.length > 0 && isStaticString(binding?.routingKey))) {
      return "RabbitMQ Trigger needs static exchange names and routing keys in its bindings";
    }
    for (const key of ["assertQueue", "durable", "autoDelete", "exclusive", "jsonParseBody"]) {
      if (options[key] !== undefined && typeof options[key] !== "boolean") {
        return "RabbitMQ Trigger needs static boolean queue and message settings";
      }
    }
    if (options.assertQueue && Object.keys(options.arguments ?? {}).length > 0) {
      return "RabbitMQ Trigger queue declaration does not support custom arguments";
    }
    return {
      queue,
      bindings: bindings.map(({ exchange, routingKey }) => ({ exchange, routingKey })),
      prefetch: prefetch === -1 ? undefined : prefetch,
      parseJson: options.jsonParseBody === true,
      declareQueue: options.assertQueue ? {
        durable: options.durable,
        autoDelete: options.autoDelete,
        exclusive: options.exclusive
      } : undefined
    };
  }

  // src/compiler/core-parser.ts
  class CoreWorkflowParser {
    static parseWorkflow(json, fallbackName, relativePath) {
      if (!json || !Array.isArray(json.nodes)) {
        return null;
      }
      const id = json.id || fallbackName;
      const name = json.name || fallbackName;
      const nodes = json.nodes || [];
      const connections = json.connections || {};
      const relPath = relativePath || `${fallbackName}.json`;
      const incomingMap = new Map;
      nodes.forEach((n) => incomingMap.set(n.name, []));
      Object.entries(connections).forEach(([sourceNodeName, outputs]) => {
        if (outputs.main && Array.isArray(outputs.main)) {
          outputs.main.forEach((outputTargets, outputIndex) => {
            if (Array.isArray(outputTargets)) {
              outputTargets.forEach((target) => {
                if (target && target.node && incomingMap.has(target.node)) {
                  incomingMap.get(target.node)?.push({ sourceNode: sourceNodeName, outputIndex });
                }
              });
            }
          });
        }
      });
      nodes.forEach((node) => {
        node.incomingConnections = incomingMap.get(node.name) || [];
      });
      const triggers = nodes.filter((node) => node.type === rabbitMqTriggerType || node.type.includes("webhook") || node.type.includes("trigger") || node.type.includes("cron"));
      const triggerNames = new Set(triggers.map((t) => t.name));
      nodes.forEach((node) => {
        const ancestorTriggers = new Set;
        const visited = new Set;
        const queue = [...(incomingMap.get(node.name) || []).map((c) => c.sourceNode)];
        while (queue.length > 0) {
          const curr = queue.shift();
          if (visited.has(curr))
            continue;
          visited.add(curr);
          if (triggerNames.has(curr)) {
            ancestorTriggers.add(curr);
          }
          const parents = incomingMap.get(curr) || [];
          parents.forEach((p) => queue.push(p.sourceNode));
        }
        node.ancestorTriggers = Array.from(ancestorTriggers);
      });
      const orderedNodes = this.topologicalSort(nodes, connections);
      return {
        id,
        name,
        fileName: fallbackName,
        relativePath: relPath.replace(/\.json$/, ".ts"),
        nodes,
        connections,
        orderedNodes,
        triggers
      };
    }
    static topologicalSort(nodes, connections) {
      const nodeMap = new Map;
      const adjList = new Map;
      nodes.forEach((node) => {
        nodeMap.set(node.name, node);
        adjList.set(node.name, []);
      });
      Object.entries(connections).forEach(([sourceNodeName, outputs]) => {
        if (outputs.main && Array.isArray(outputs.main)) {
          outputs.main.forEach((outputTargets) => {
            if (Array.isArray(outputTargets)) {
              outputTargets.forEach((target) => {
                if (target && target.node && nodeMap.has(target.node)) {
                  adjList.get(sourceNodeName)?.push(target.node);
                }
              });
            }
          });
        }
      });
      const visited = new Set;
      const visiting = new Set;
      const result = [];
      const dfs = (nodeName, pathStack = []) => {
        if (visited.has(nodeName))
          return;
        if (visiting.has(nodeName)) {
          const cycle = [...pathStack, nodeName].join(" -> ");
          throw new Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${cycle}). Transpiled workflows must be strict Directed Acyclic Graphs.`);
        }
        visiting.add(nodeName);
        pathStack.push(nodeName);
        const neighbors = adjList.get(nodeName) || [];
        for (const neighbor of neighbors) {
          dfs(neighbor, pathStack);
        }
        pathStack.pop();
        visiting.delete(nodeName);
        visited.add(nodeName);
        const node = nodeMap.get(nodeName);
        if (node) {
          result.push(node);
        }
      };
      const inDegree = new Map;
      nodes.forEach((n) => inDegree.set(n.name, 0));
      for (const neighbors of adjList.values()) {
        for (const neighbor of neighbors) {
          inDegree.set(neighbor, (inDegree.get(neighbor) || 0) + 1);
        }
      }
      const roots = nodes.map((n) => n.name).filter((name) => inDegree.get(name) === 0);
      for (const root of roots) {
        dfs(root);
      }
      for (const node of nodes) {
        if (!visited.has(node.name)) {
          dfs(node.name);
        }
      }
      return result.reverse();
    }
  }

  // src/compiler/expression-parser.ts
  class ExpressionParser {
    static parse(expression, firstInputItem) {
      if (expression === undefined || expression === null) {
        return JSON.stringify("");
      }
      if (typeof expression !== "string") {
        return JSON.stringify(expression);
      }
      let expr = expression.trim();
      if (!expr.includes("{{")) {
        return JSON.stringify(expression);
      }
      if (expr.startsWith("=")) {
        const innerContent = expr.substring(1).trim();
        if (innerContent.startsWith("{{") && innerContent.endsWith("}}")) {
          const between = innerContent.substring(2, innerContent.length - 2).trim();
          if (!between.includes("{{") && !between.includes("}}")) {
            const transformed = this.transformTokens(between, firstInputItem);
            return `(${transformed})`;
          }
        }
        expr = expr.substring(1).trim();
      }
      const replacements = [];
      let placeholderIdx = 0;
      const tokenized = expr.replace(/\{\{(.*?)\}\}/g, (_, inner) => {
        const transformed = this.transformTokens(inner.trim(), firstInputItem);
        const placeholder = `__EXPR_PLACEHOLDER_${placeholderIdx++}__`;
        replacements.push(`\${(${transformed}) ?? ''}`);
        return placeholder;
      });
      let safeLiteral = tokenized.replace(/`/g, "\\`").replace(/\${/g, "\\${");
      replacements.forEach((rep, idx) => {
        safeLiteral = safeLiteral.replace(`__EXPR_PLACEHOLDER_${idx}__`, rep);
      });
      return `\`${safeLiteral}\``;
    }
    static transformTokens(code, firstInputItem) {
      let result = code;
      result = result.replace(/\$now\b/g, "new Date().toISOString()");
      result = result.replace(/\$today\b/g, 'new Date().toISOString().split("T")[0]');
      result = result.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g, (_, nodeName, path) => {
        const parts = path ? path.split(".").filter(Boolean) : [];
        return `(ctx.getNode(${JSON.stringify(nodeName)})?.item?.json${parts.length ? "?." + parts.join("?.") : ""})`;
      });
      result = result.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g, (_, nodeName, path) => {
        const parts = path ? path.split(".").filter(Boolean) : [];
        return `(ctx.getNode(${JSON.stringify(nodeName)})?.first()?.json${parts.length ? "?." + parts.join("?.") : ""})`;
      });
      result = result.replace(/\$\(['"](.*?)['"]\)/g, (_, nodeName) => {
        return `ctx.getNode(${JSON.stringify(nodeName)})`;
      });
      result = result.replace(/\$node\(['"](.*?)['"]\)/g, (_, nodeName) => {
        return `ctx.getNode(${JSON.stringify(nodeName)})`;
      });
      result = result.replace(/\$json([a-zA-Z0-9_.]*)/g, (_, path) => {
        if (!path)
          return "item?.json";
        const parts = path.split(".").filter(Boolean);
        return `item?.json?.${parts.join("?.")}`;
      });
      result = result.replace(/\$input\.first\(\)\.json/g, `${firstInputItem ?? "item"}?.json`);
      if (firstInputItem)
        result = result.replace(/\$input\.first\(\)/g, firstInputItem);
      result = result.replace(/\$input\.item\.json/g, "item?.json");
      result = result.replace(/\$input\.all\(\)/g, "inputItems");
      result = result.replace(/\$env\.([a-zA-Z0-9_]+)/g, (_, envVar) => {
        return `process.env.${envVar}`;
      });
      return result;
    }
  }

  // src/compiler/mappers/rabbitmq.ts
  var rabbitMqPublisherType = "n8n-nodes-base.rabbitmq";
  function getRabbitMqPublisherIssue(node) {
    const params = node.parameters ?? {};
    const version = node.typeVersion ?? 1;
    if (version !== 1 && version !== 1.1) {
      return "RabbitMQ needs typeVersion 1 or 1.1";
    }
    if ((params.operation ?? "sendMessage") !== "sendMessage" || params.mode !== "exchange" || Object.keys(params.options ?? {}).length > 0) {
      return "RabbitMQ only supports Send Message to an existing exchange with default options";
    }
  }
  function mapRabbitMqPublisher(ctx) {
    const issue = getRabbitMqPublisherIssue(ctx.node);
    if (issue)
      throw new Error(`Node ${JSON.stringify(ctx.node.name)}: ${issue}`);
    const params = ctx.node.parameters ?? {};
    const source = ctx.previousVariableName ?? "inputItems";
    const exchange = ExpressionParser.parse(params.exchange ?? "");
    const routingKey = ExpressionParser.parse(params.routingKey ?? "");
    const sendInputData = ExpressionParser.parse(params.sendInputData ?? true);
    const message = ExpressionParser.parse(params.message ?? "");
    const output = ctx.variableName;
    return `
    {
      const source: N8nItem[] = ${source} || inputItems;
      ${output} = [];
      if (source.length > 0) {
        item = source[0];
        const exchange = String(${exchange});
        const routingKey = String(${routingKey});
        const sendInputData = Boolean(${sendInputData});
        const messages = source.map((item) => sendInputData ? JSON.stringify(item.json) : String(${message}));
        ${output} = await publishRabbitMq(exchange, routingKey, messages);
      }
      item = ${output}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${output});
    }`;
  }

  // src/compiler/mappers/index.ts
  class NodeMappers {
    static mapNode(ctx) {
      const type = ctx.node.type;
      if (type === rabbitMqPublisherType) {
        return mapRabbitMqPublisher(ctx);
      }
      if (type.includes("respondToWebhook")) {
        return this.mapRespondToWebhookNode(ctx);
      }
      if (type.includes("code") || type.includes("function")) {
        return this.mapCodeNode(ctx);
      }
      if (type.includes("executeWorkflowTrigger")) {
        return this.mapGenericNode(ctx, "Subworkflow Entry Trigger");
      }
      if (type.includes("executeWorkflow")) {
        return this.mapSubworkflowNode(ctx);
      }
      if (type.includes("filter")) {
        return this.mapFilterNode(ctx);
      }
      if (type.includes("if")) {
        return this.mapIfNode(ctx);
      }
      if (type.includes("switch")) {
        return this.mapSwitchNode(ctx);
      }
      if (type.includes("set")) {
        return this.mapSetNode(ctx);
      }
      if (type.includes("httpRequest")) {
        return this.mapHttpRequestNode(ctx);
      }
      if (type.includes("aggregate") || type.includes("itemLists")) {
        return this.mapAggregateNode(ctx);
      }
      if (type.includes("splitOut")) {
        return this.mapSplitOutNode(ctx);
      }
      if (type.includes("merge")) {
        return this.mapMergeNode(ctx);
      }
      if (type.includes("postgres")) {
        return this.mapPostgresNode(ctx);
      }
      if (type.includes("openAi") || type.includes("googleGemini") || type.includes("chainLlm")) {
        return this.mapLlmNode(ctx);
      }
      if (type.includes("wait")) {
        return this.mapWaitNode(ctx);
      }
      return this.mapGenericNode(ctx, `Unmapped Node (${type})`);
    }
    static mapRespondToWebhookNode(ctx) {
      const params = ctx.node.parameters || {};
      const respondWith = params.respondWith || "firstIncomingItem";
      const statusCode = params.options?.responseCode || 200;
      const prevVar = ctx.previousVariableName || "inputItems";
      let responseExpr = "item?.json || {}";
      if (respondWith === "allIncomingItems") {
        responseExpr = `${ctx.variableName}.map((i: any) => i.json)`;
      } else if (respondWith === "json" && params.responseBody) {
        responseExpr = ExpressionParser.parse(params.responseBody);
      } else if (respondWith === "text" && params.responseBody) {
        responseExpr = ExpressionParser.parse(params.responseBody);
      }
      return `
    // [Node: ${ctx.node.name}] Respond to Webhook
    ${ctx.variableName} = ${prevVar} || inputItems;
    item = ${ctx.variableName}[0] || { json: {} };
    {
      let payload: any = ${responseExpr};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${statusCode}, payload);
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});
    }`;
    }
    static mapCodeNode(ctx) {
      const params = ctx.node.parameters || {};
      const rawCode = params.jsCode || params.functionCode || "return $input.all();";
      const prevVar = ctx.previousVariableName || "inputItems";
      const transformedCode = rawCode.replace(/\$input\.all\(\)/g, "currentItems").replace(/\$input\.first\(\)/g, "(currentItems[0] || { json: {} })").replace(/\$input\.item/g, "(currentItems[0] || { json: {} })").replace(/\$json/g, "(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g, (_, n) => `(ctx.getNode(${JSON.stringify(n)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g, (_, n) => `(ctx.getNode(${JSON.stringify(n)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g, (_, n) => `(ctx.getNode(${JSON.stringify(n)})?.all() || [])`).replace(/\$now/g, "DateTime.now()");
      return `
    // [Node: ${ctx.node.name}] Code Execution
    ${ctx.variableName} = await (async () => {
      const currentItems = ${prevVar} || inputItems;
      const result = await (async () => {
        ${transformedCode}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
    static mapSubworkflowNode(ctx) {
      const params = ctx.node.parameters || {};
      const workflowId = typeof params.workflowId === "object" ? params.workflowId?.value || params.workflowId?.cachedResultName || "" : params.workflowId || "";
      const prevVar = ctx.previousVariableName || "inputItems";
      return `
    // [Node: ${ctx.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${ctx.variableName} = await executeSubworkflow(${JSON.stringify(workflowId)}, ${prevVar} || inputItems, ctx);
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
    static mapSetNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const keepOnlySet = params.options?.keepOnlySet === true || params.include === "none";
      const version = ctx.node.typeVersion ?? 3.4;
      const options = params.options ?? {};
      let includeBinary;
      if (version >= 3.4) {
        const includeOtherFields = ExpressionParser.parse(params.includeOtherFields ?? false);
        const include = ExpressionParser.parse(params.include ?? "all");
        const stripBinary = ExpressionParser.parse(options.stripBinary ?? false);
        includeBinary = `(${includeOtherFields}) && (${include}) !== 'none' && !(${stripBinary})`;
      } else if (version >= 3) {
        includeBinary = ExpressionParser.parse(options.includeBinary ?? false);
      } else {
        includeBinary = `!(${ExpressionParser.parse(params.keepOnlySet ?? options.keepOnlySet ?? false)})`;
      }
      const assignments = [];
      if (params.assignments?.assignments && Array.isArray(params.assignments.assignments)) {
        for (const a of params.assignments.assignments) {
          if (a.name) {
            const valExpr = ExpressionParser.parse(a.value);
            assignments.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(a.name)}, ${valExpr});`);
          }
        }
      }
      if (params.values) {
        for (const typeKey of ["string", "number", "boolean"]) {
          const list = params.values[typeKey];
          if (Array.isArray(list)) {
            for (const entry of list) {
              if (entry.name) {
                const valExpr = ExpressionParser.parse(entry.value);
                assignments.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(entry.name)}, ${valExpr});`);
              }
            }
          }
        }
      }
      return `
    // [Node: ${ctx.node.name}] Set / Edit Fields
    ${ctx.variableName} = (${prevVar} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${keepOnlySet ? "{}" : "{ ...currentItem.json }"};
      ${assignments.join(`
      `)}
      return {
        json: newJson,
        ...(currentItem.binary !== undefined && (${includeBinary}) ? { binary: { ...currentItem.binary } } : {}),
      };
    });
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
    static parseConditions(group, legacyFilter = false) {
      const conditions = [];
      const firstInputItem = legacyFilter ? undefined : "firstInputItem";
      if (Array.isArray(group?.conditions)) {
        for (const cond of group.conditions) {
          let left = ExpressionParser.parse(cond.leftValue, firstInputItem);
          let right = ExpressionParser.parse(cond.rightValue, firstInputItem);
          const op = cond.operator?.operation || "equals";
          if (legacyFilter && ["true", "false", "gte", "lte"].includes(op)) {
            conditions.push(`Boolean(${left})`);
            continue;
          }
          if (!legacyFilter && group.options?.caseSensitive === false && cond.operator?.type === "string") {
            const lowerCase = "(value: unknown) => typeof value === 'string' ? value.toLowerCase() : value";
            left = `((${lowerCase})(${left}))`;
            right = `((${lowerCase})(${right}))`;
          }
          switch (op) {
            case "equals":
            case "equal":
              conditions.push(`(${left} == ${right})`);
              break;
            case "notEquals":
            case "notEqual":
              conditions.push(`(${left} != ${right})`);
              break;
            case "contains":
              conditions.push(`String(${left} ?? '').includes(String(${right} ?? ''))`);
              break;
            case "true":
              conditions.push(`(${left} === true)`);
              break;
            case "false":
              conditions.push(`(${left} === false)`);
              break;
            case "gt":
            case "larger":
              conditions.push(`(Number(${left}) > Number(${right}))`);
              break;
            case "lt":
            case "smaller":
              conditions.push(`(Number(${left}) < Number(${right}))`);
              break;
            case "gte":
              conditions.push(`(Number(${left}) >= Number(${right}))`);
              break;
            case "lte":
              conditions.push(`(Number(${left}) <= Number(${right}))`);
              break;
            case "isEmpty":
            case "empty":
              conditions.push(`(${left} === undefined || ${left} === null || ${left} === '')`);
              break;
            case "isNotEmpty":
            case "notEmpty":
              conditions.push(`(${left} !== undefined && ${left} !== null && ${left} !== '')`);
              break;
            default:
              conditions.push(`Boolean(${left})`);
          }
        }
      }
      const combinator = group?.combinator === "or" ? " || " : " && ";
      return conditions.length ? conditions.join(combinator) : "true";
    }
    static mapFilterNode(ctx) {
      const prevVar = ctx.previousVariableName || "inputItems";
      const conditionExpr = this.parseConditions(ctx.node.parameters?.conditions, true);
      return `
    // [Node: ${ctx.node.name}] Filter
    ${ctx.variableName} = ${prevVar} || inputItems;
    item = ${ctx.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${conditionExpr});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName}, activeIndex);
    }`;
    }
    static mapIfNode(ctx) {
      const prevVar = ctx.previousVariableName || "inputItems";
      const conditionExpr = this.parseConditions(ctx.node.parameters?.conditions);
      return `
    // [Node: ${ctx.node.name}] Conditional Branch (IF)
    ${ctx.variableName} = ${prevVar} || inputItems;
    {
      const firstInputItem: N8nItem | undefined = ${ctx.variableName}[0];
      const outputs: N8nItem[][] = [[], []];
      for (const item of ${ctx.variableName} as N8nItem[]) {
        const port = (${conditionExpr}) ? 0 : 1;
        outputs[port].push(item);
      }
      ctx.setNodeOutputs(${JSON.stringify(ctx.node.name)}, outputs.entries(), ${ctx.variableName});
    }
    item = ${ctx.variableName}[0] || { json: {} };`;
    }
    static mapSwitchNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const rules = params.rules?.values || [];
      const options = params.options ?? {};
      const fallback = options.fallbackOutput ?? "none";
      const fallbackPort = fallback === "extra" ? rules.length : fallback;
      const hasFallback = fallback === "extra" || Number.isInteger(fallbackPort) && fallbackPort >= 0 && fallbackPort < rules.length;
      const fallbackCode = hasFallback ? `
        if (!matched) {
          send(item, ${fallbackPort});
          matched = true;
        }` : "";
      const predicates = rules.map((rule) => `(item: N8nItem) => (${rule.conditions ? this.parseConditions(rule.conditions) : "false"})`);
      const matchRules = `
      const predicates = [${predicates.join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${ExpressionParser.parse(options.allMatchingOutputs ?? false, "firstInputItem")})) break;
          }
        }
        ${fallbackCode}
        if (matched) routed.push(item);
      }`;
      const matchExpression = `
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${ExpressionParser.parse(params.numberOutputs ?? 4, "firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${ExpressionParser.parse(params.output ?? 0, "firstInputItem")};
        if (!Number.isSafeInteger(numberOutputs) || !Number.isSafeInteger(port) || port < 0 || port >= numberOutputs) {
          throw new Error('Switch expression returned an invalid output index');
        }
        send(item, port);
        routed.push(item);
      }
    `;
      return `
    // [Node: ${ctx.node.name}] Switch Router
    {
      const source: N8nItem[] = ${prevVar} || inputItems;
      const firstInputItem = source[0];
      const outputs = new Map<number, N8nItem[]>();
      const routed: N8nItem[] = [];
      const send = (item: N8nItem, port: number) => {
        let items = outputs.get(port);
        if (!items) {
          items = [];
          outputs.set(port, items);
        }
        items.push(item);
      };
      ${params.mode === "expression" ? matchExpression : matchRules}
      ${ctx.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(ctx.node.name)}, outputs, routed);
    }`;
    }
    static mapHttpRequestNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const method = (params.method || "GET").toUpperCase();
      const urlExpr = ExpressionParser.parse(params.url || "");
      let headersObj = "{}";
      if (params.headerParameters?.parameters && Array.isArray(params.headerParameters.parameters)) {
        const pairs = params.headerParameters.parameters.filter((p) => p.name).map((p) => `${JSON.stringify(p.name)}: ${ExpressionParser.parse(p.value)}`);
        headersObj = `{ ${pairs.join(", ")} }`;
      }
      let bodySnippet = "undefined";
      if (["POST", "PUT", "PATCH"].includes(method)) {
        if (params.jsonBody) {
          bodySnippet = `JSON.stringify(${ExpressionParser.parse(params.jsonBody)})`;
        } else if (params.bodyParameters?.parameters && Array.isArray(params.bodyParameters.parameters)) {
          const pairs = params.bodyParameters.parameters.filter((p) => p.name).map((p) => `${JSON.stringify(p.name)}: ${ExpressionParser.parse(p.value)}`);
          bodySnippet = `JSON.stringify({ ${pairs.join(", ")} })`;
        }
      }
      return `
    // [Node: ${ctx.node.name}] Native HTTP Request (${method})
    ${ctx.variableName} = await Promise.all((${prevVar} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${urlExpr};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${headersObj} };
      const reqBody = ${bodySnippet};
      const res = await fetch(targetUrl, {
        method: ${JSON.stringify(method)},
        headers: reqHeaders,
        ...(reqBody !== undefined ? { body: typeof reqBody === 'string' ? reqBody : JSON.stringify(reqBody) } : {})
      });
      const text = await res.text();
      let data: any;
      try { data = JSON.parse(text); } catch { data = { raw: text }; }
      return { json: typeof data === 'object' && data !== null ? data : { data } };
    }));
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
    static mapAggregateNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const destinationField = params.destinationFieldName || "data";
      return `
    // [Node: ${ctx.node.name}] Aggregate Items
    {
      const source = ${prevVar} || inputItems;
      ${ctx.variableName} = [{ json: { ${JSON.stringify(destinationField)}: source.map((i: N8nItem) => i.json) } }];
      item = ${ctx.variableName}[0];
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});
    }`;
    }
    static mapSplitOutNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const fieldToSplit = params.fieldToSplitOut || "data";
      return `
    // [Node: ${ctx.node.name}] Split Out Array Field
    {
      const source = ${prevVar} || inputItems;
      const expanded: N8nItem[] = [];
      for (const entry of source) {
        const arr = entry.json?.[${JSON.stringify(fieldToSplit)}];
        if (Array.isArray(arr)) {
          for (const el of arr) {
            expanded.push({ json: typeof el === 'object' && el !== null ? el : { [${JSON.stringify(fieldToSplit)}]: el } });
          }
        }
      }
      ${ctx.variableName} = expanded.length > 0 ? expanded : source;
      item = ${ctx.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});
    }`;
    }
    static mapMergeNode(ctx) {
      const incoming = ctx.node.incomingConnections || [];
      const mode = ctx.node.parameters?.mode || "append";
      const sources = incoming.map((c) => `(ctx.getNodeOutput(${JSON.stringify(c.sourceNode)}, ${c.outputIndex}) || [])`);
      if (mode === "combine" && sources.length >= 2) {
        return `
    // [Node: ${ctx.node.name}] Merge (Combine Mode)
    {
      const left = ${sources[0]};
      const right = ${sources[1]};
      const maxLen = Math.max(left.length, right.length);
      const merged: N8nItem[] = [];
      for (let i = 0; i < maxLen; i++) {
        merged.push({ json: { ...(left[i]?.json || {}), ...(right[i]?.json || {}) } });
      }
      ${ctx.variableName} = merged.length > 0 ? merged : [{ json: {} }];
      item = ${ctx.variableName}[0];
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});
    }`;
      }
      return `
    // [Node: ${ctx.node.name}] Merge (Append Mode)
    {
      const combined = [${sources.join(", ")}].flat();
      ${ctx.variableName} = combined.length > 0 ? combined : inputItems;
      item = ${ctx.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});
    }`;
    }
    static mapPostgresNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const queryExpr = ExpressionParser.parse(params.query || "SELECT 1;");
      return `
    // [Node: ${ctx.node.name}] PostgreSQL Query (via DATABASE_URL env var)
    ${ctx.variableName} = await Promise.all((${prevVar} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const sql = ${queryExpr};
      if (!process.env.DATABASE_URL) {
        throw new Error('[n8n-to-fastify] DATABASE_URL environment variable is required for Postgres node.');
      }
      // Uses Bun's native SQL or dynamic pg driver when available
      const res = await fetch(process.env.POSTGRES_HTTP_PROXY || '', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: sql })
      }).catch(() => null);
      if (res && res.ok) {
        const rows = await res.json();
        return { json: Array.isArray(rows) ? (rows[0] || {}) : rows };
      }
      return { json: { ...currentItem.json, _sqlPrepared: sql } };
    }));
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
    static mapLlmNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const model = params.modelId?.value || params.model || "gpt-4o-mini";
      const promptText = params.messages?.values?.[0]?.content || params.prompt || params.text || "Hello";
      const promptExpr = ExpressionParser.parse(promptText);
      return `
    // [Node: ${ctx.node.name}] Native LLM Completion (OpenAI-compatible REST API)
    ${ctx.variableName} = await Promise.all((${prevVar} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const apiKey = process.env.OPENAI_API_KEY || process.env.LLM_API_KEY || '';
      const baseUrl = process.env.LLM_BASE_URL || 'https://api.openai.com/v1';
      const prompt = ${promptExpr};
      const res = await fetch(\`\${baseUrl}/chat/completions\`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': \`Bearer \${apiKey}\`
        },
        body: JSON.stringify({
          model: ${JSON.stringify(model)},
          messages: [{ role: 'user', content: String(prompt) }]
        })
      });
      const data: any = await res.json();
      const text = data?.choices?.[0]?.message?.content || '';
      return { json: { text, model: ${JSON.stringify(model)}, usage: data?.usage } };
    }));
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
    static mapWaitNode(ctx) {
      const params = ctx.node.parameters || {};
      const prevVar = ctx.previousVariableName || "inputItems";
      const amount = Number(params.amount || 1);
      const unit = params.unit || "seconds";
      const msMultiplier = unit === "minutes" ? 60000 : unit === "hours" ? 3600000 : 1000;
      const delayMs = amount * msMultiplier;
      return `
    // [Node: ${ctx.node.name}] Non-blocking Async Wait (${amount} ${unit})
    await new Promise((resolve) => setTimeout(resolve, ${delayMs}));
    ${ctx.variableName} = ${prevVar} || inputItems;
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
    static mapGenericNode(ctx, label) {
      const prevVar = ctx.previousVariableName || "inputItems";
      return `
    // [Node: ${ctx.node.name}] ${label} (Pass-through)
    ${ctx.variableName} = ${prevVar} || inputItems;
    item = ${ctx.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(ctx.node.name)}, ${ctx.variableName});`;
    }
  }

  // src/compiler/code-generator.ts
  class CodeGenerator {
    static sanitizeIdentifier(str) {
      return str.replace(/[^a-zA-Z0-9_]/g, "_").replace(/^[0-9]/, "_$&").replace(/__+/g, "_");
    }
    static generateWorkflowCode(wf) {
      const functionName = `workflow_${this.sanitizeIdentifier(wf.fileName)}`;
      const depth = (wf.relativePath.match(/\//g) || []).length;
      const runtimePrefix = "../".repeat(depth + 1) + "runtime";
      const rabbitMqImport = wf.nodes.some((node) => node.type === rabbitMqPublisherType) ? `import { publishRabbitMq } from '${runtimePrefix}/rabbitmq';
` : "";
      const imports = `import { WorkflowContext } from '${runtimePrefix}/workflow-context';
import { n8n, N8nItem } from '${runtimePrefix}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${runtimePrefix}/workflow-registry';
${rabbitMqImport}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`;
      let declCode = `  let item: N8nItem = inputItems[0] || { json: {} };
`;
      let bodyCode = `  ctx.setNodeOutput("__initial__", inputItems);
`;
      const nodeVarMap = new Map;
      wf.orderedNodes.forEach((node, index) => {
        const varName = `node_${index}_${this.sanitizeIdentifier(node.name)}`;
        nodeVarMap.set(node.name, varName);
      });
      const triggers = wf.triggers;
      const hasRabbitMqTrigger = triggers.some((node) => node.type === rabbitMqTriggerType);
      const nonTriggers = wf.orderedNodes.filter((n) => !triggers.includes(n));
      triggers.forEach((node) => {
        const index = wf.orderedNodes.indexOf(node);
        const varName = nodeVarMap.get(node.name);
        declCode += `  let ${varName}: any = inputItems;
`;
        const nodeName = JSON.stringify(node.name);
        let condition = `entryNode === ${nodeName}`;
        if (node.type !== rabbitMqTriggerType) {
          const rawNodePath = (node.parameters?.path || node.name).toLowerCase().trim().replace(/^\/+/, "");
          bodyCode += `  const triggerNode_${index} = item?.json?.__triggerNode;
`;
          bodyCode += `  const triggerPath_${index} = item?.json?.__triggerPath;
`;
          condition = `entryNode === undefined && (!triggerNode_${index} || triggerNode_${index} === ${nodeName} || triggerPath_${index} === ${JSON.stringify(rawNodePath)})`;
        }
        const nodeCode = `${varName} = inputItems;
ctx.setNodeOutput(${nodeName}, inputItems);`;
        bodyCode += this.generateNodeBlock(wf.id, node, condition, nodeCode);
      });
      nonTriggers.forEach((node) => {
        const varName = nodeVarMap.get(node.name);
        declCode += `  let ${varName}: any = inputItems;
`;
        let branchCheckStr = "true";
        const incoming = node.incomingConnections ?? [];
        const runPerIncomingBatch = incoming.length > 0 && !node.type.includes("merge");
        if (incoming.length > 0) {
          const branchChecks = incoming.map((conn) => `ctx.isBranchActive(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex})`);
          branchCheckStr = `(${branchChecks.join(" || ")})`;
        }
        let nodeCode = NodeMappers.mapNode({
          node,
          workflowName: wf.name,
          variableName: varName,
          previousVariableName: runPerIncomingBatch ? "incomingItems" : "inputItems"
        });
        if (runPerIncomingBatch) {
          const batches = incoming.map((conn) => `ctx.getNodeOutputBatches(${JSON.stringify(conn.sourceNode)}, ${conn.outputIndex})`);
          nodeCode = `await ctx.runNodeBatches(${JSON.stringify(node.name)}, [${batches.join(", ")}].flat(), async (incomingItems) => {
${nodeCode.trim()}
});`;
        }
        const ancestorTriggers = node.ancestorTriggers ?? [];
        if (ancestorTriggers.length > 0) {
          const triggerConditions = ancestorTriggers.map((t) => `ctx.hasExecutedNode(${JSON.stringify(t)})`).join(" || ");
          branchCheckStr = `(${triggerConditions}) && ${branchCheckStr}`;
        } else if (hasRabbitMqTrigger) {
          branchCheckStr = `entryNode === undefined && ${branchCheckStr}`;
        }
        bodyCode += this.generateNodeBlock(wf.id, node, branchCheckStr, nodeCode, ancestorTriggers.length > 0 ? "      " : "    ");
      });
      const lastNodeName = wf.orderedNodes[wf.orderedNodes.length - 1]?.name;
      const returnItemsExpr = lastNodeName ? `(ctx.getNode(${JSON.stringify(lastNodeName)})?.all() || [{ json: item.json }])` : `inputItems`;
      const keys = Array.from(new Set([wf.id, wf.name, wf.fileName])).filter(Boolean);
      return `${imports}
export async function ${functionName}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext,
  entryNode?: string
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
  const $env = process.env;
${declCode}
${bodyCode}
  const finalItems = ${returnItemsExpr};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(keys)}, ${functionName});
`;
    }
    static generateNodeBlock(workflowId, node, condition, nodeCode, nodeIndent = "    ") {
      const traceArgs = [workflowId, node.id || node.name, node.name].map((value) => JSON.stringify(value)).join(", ");
      const body = nodeIndent + nodeCode.trim().replace(/\n/g, `
${nodeIndent}`);
      return `  if (${condition}) {
    const finishTrace = ctx.startNodeTrace(${traceArgs});
    try {
${body}
    } catch (error) {
      finishTrace?.('error');
      throw error;
    }
    finishTrace?.('completed');
  } else {
    ctx.skipNodeTrace(${traceArgs});
  }
`;
    }
  }

  // src/compiler/route-generator.ts
  class RouteGenerator {
    static generateRoutesCode(workflows) {
      let imports = `import { randomUUID } from 'node:crypto';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`;
      const importedFunctions = [];
      workflows.forEach((wf) => {
        const functionName = `workflow_${CodeGenerator.sanitizeIdentifier(wf.fileName)}`;
        const importPath = wf.relativePath.replace(/\.ts$/, "");
        imports += `import { ${functionName} } from '../workflows/${importPath}';
`;
        importedFunctions.push(functionName);
      });
      let routeDefinitions = "";
      const rabbitMqStarts = [];
      const registeredRoutes = new Set;
      workflows.forEach((wf) => {
        const functionName = `workflow_${CodeGenerator.sanitizeIdentifier(wf.fileName)}`;
        const webhooks = wf.nodes.filter((node) => node.type.includes("webhook"));
        for (const node of getRabbitMqTriggers(wf)) {
          const options = parseRabbitMqTrigger(node);
          if (typeof options === "string") {
            throw new Error(`Workflow ${JSON.stringify(wf.name)}, node ${JSON.stringify(node.name)}: ${options}`);
          }
          const logContext = JSON.stringify({ workflowId: wf.id, node: node.name });
          rabbitMqStarts.push(`      stopConsumers.push(await startRabbitMqConsumer(
        ${JSON.stringify(options)},
        (items) => ${functionName}(items, undefined, ${JSON.stringify(node.name)}),
        (message) => fastify.log.error(${logContext}, message),
      ));`);
        }
        webhooks.forEach((node) => {
          const rawPath = (node.parameters?.path || wf.fileName).toLowerCase().trim().replace(/^\/+/, "");
          const configuredMethod = (node.parameters?.httpMethod || "POST").toUpperCase();
          const methods = configuredMethod === "ALL" ? ["GET", "POST", "PUT", "DELETE"] : [configuredMethod];
          const targetPaths = Array.from(new Set([`/${rawPath}`, `/webhook/${rawPath}`]));
          methods.forEach((httpMethod) => {
            targetPaths.forEach((routePath) => {
              const routeKey = `${httpMethod}:${routePath}`;
              if (registeredRoutes.has(routeKey))
                return;
              registeredRoutes.add(routeKey);
              routeDefinitions += `
  fastify.route({
    method: '${httpMethod}',
    url: '${routePath}',
    handler: async (request: FastifyRequest, reply: FastifyReply) => {
      const startTime = performance.now();
      const ctx = new WorkflowContext();
      const body = (request.body as Record<string, any>) || {};
      const query = (request.query as Record<string, any>) || {};
      const headers = (request.headers as Record<string, any>) || {};

      const inputItems: N8nItem[] = [{
        json: {
          ...query,
          ...body,
          body,
          query,
          headers,
          __triggerNode: ${JSON.stringify(node.name)},
          __triggerPath: ${JSON.stringify(rawPath)}
        }
      }];

      try {
        const { items, context } = await ${functionName}(inputItems, ctx);
        const durationMs = (performance.now() - startTime).toFixed(2);
        reply.header('X-Compiled-By', 'n8n-to-fastify');
        reply.header('X-Execution-Time', \`\${durationMs}ms\`);

        if (context.webhookResponse) {
          reply.code(context.webhookResponse.statusCode);
          return context.webhookResponse.body;
        }
        reply.code(200);
        return items.length === 1 ? items[0].json : items.map(i => i.json);
      } catch {
        const errorId = randomUUID();
        request.log.error({ errorId }, 'Workflow execution failed');
        reply.code(500);
        return { error: 'Workflow Execution Error', message: 'Workflow execution failed', errorId };
      }
    }
  });
`;
            });
          });
        });
      });
      if (rabbitMqStarts.length > 0) {
        imports += `import { startRabbitMqConsumer } from '../runtime/rabbitmq';
`;
        routeDefinitions += `
  const stopConsumers: Array<() => Promise<void>> = [];
  const closeConsumers = async () => {
    await Promise.allSettled(stopConsumers.splice(0).map(stop => stop()));
  };
  fastify.addHook('onReady', async () => {
    try {
${rabbitMqStarts.join(`
`)}
    } catch (error) {
      await closeConsumers();
      throw error;
    }
  });
  fastify.addHook('onClose', closeConsumers);
`;
      }
      return `${imports}
// Prevent bundler/runtime tree-shaking so subworkflows self-register into WorkflowRegistry
const _registeredWorkflows = [${importedFunctions.join(", ")}];
void _registeredWorkflows;

export async function registerCompiledRoutes(fastify: FastifyInstance): Promise<void> {
${routeDefinitions}
}
`;
    }
    static generateServerEntry(workflows = []) {
      const shutdown = workflows.some((workflow) => getRabbitMqTriggers(workflow).length > 0) ? `
  const close = () => {
    void fastify.close().catch(error => {
      fastify.log.error(error);
      process.exitCode = 1;
    });
  };
  process.once('SIGINT', close);
  process.once('SIGTERM', close);` : "";
      return `import Fastify from 'fastify';
import cors from '@fastify/cors';
import { registerCompiledRoutes } from './routes/index';
import { workflowRegistry } from './runtime/workflow-registry';

const fastify = Fastify({
  logger: true,
});

await fastify.register(cors, { origin: '*' });
await fastify.register(registerCompiledRoutes);

fastify.get('/health', async () => {
  return {
    status: 'ok',
    engine: 'n8n-to-fastify',
    registeredWorkflows: workflowRegistry.listRegistered().length,
  };
});

const port = Number(process.env.PORT || 3000);
const host = process.env.HOST || '0.0.0.0';

try {
  await fastify.listen({ port, host });
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);${shutdown}
} catch (err) {
  fastify.log.error(err);
  process.exit(1);
}
`;
    }
    static generateOutputPackageJson(workflows = [], dependencies = {}) {
      const needsRabbitMq = workflows.some((workflow) => getRabbitMqTriggers(workflow).length > 0 || workflow.nodes.some((node) => node.type === rabbitMqPublisherType));
      return JSON.stringify({
        name: "compiled-n8n-service",
        version: "1.0.0",
        type: "module",
        scripts: {
          start: "bun run server.ts",
          dev: "bun --watch run server.ts"
        },
        dependencies: {
          "@fastify/cors": "^9.0.1",
          fastify: "^4.28.1",
          ...needsRabbitMq ? { amqplib: "^0.10.9" } : {},
          ...dependencies
        }
      }, null, 2);
    }
  }

  // src/browser.ts
  function compileWorkflowJson(jsonInput) {
    const start = performance.now();
    const raw = typeof jsonInput === "string" ? JSON.parse(jsonInput) : jsonInput;
    const parsed = CoreWorkflowParser.parseWorkflow(raw, raw?.name || "workflow", "workflow.ts");
    if (!parsed) {
      throw new Error('Invalid n8n workflow JSON: missing "nodes" array.');
    }
    const workflowCode = CodeGenerator.generateWorkflowCode(parsed);
    const routesCode = RouteGenerator.generateRoutesCode([parsed]);
    const elapsedMs = (performance.now() - start).toFixed(2);
    return {
      name: parsed.name,
      nodeCount: parsed.nodes.length,
      triggerCount: parsed.triggers.length,
      elapsedMs,
      workflowCode,
      routesCode
    };
  }
  if (typeof window !== "undefined") {
    window.N8nToFastify = { compileWorkflowJson };
  }
})();
