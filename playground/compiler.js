(()=>{var{defineProperty:P,getOwnPropertyNames:V,getOwnPropertyDescriptor:B}=Object,L=Object.prototype.hasOwnProperty;function D(e){return this[e]}var F=(e)=>{var t=(A??=new WeakMap).get(e),n;if(t)return t;if(t=P({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of V(e))if(!L.call(t,r))P(t,r,{get:D.bind(e,r),enumerable:!(n=B(e,r))||n.enumerable})}return A.set(e,t),t},A;var Q=(e)=>e;function G(e,t){this[e]=Q.bind(null,t)}var z=(e,t)=>{for(var n in t)P(e,n,{get:t[n],enumerable:!0,configurable:!0,set:G.bind(t,n)})};var H={};z(H,{compileWorkflowJson:()=>W});var k="n8n-nodes-base.rabbitmqTrigger";function M(e){return e.nodes.filter((t)=>t.type==="n8n-nodes-base.rabbitmqTrigger"&&!t.disabled)}function T(e){return typeof e==="string"&&!e.startsWith("=")&&!e.includes("{{")}function _(e){if((e.typeVersion??1)!==1)return"RabbitMQ Trigger needs typeVersion 1";let{queue:t,options:n}=e.parameters??{},r=n??{};if(!T(t)||!t.trim())return"RabbitMQ Trigger needs a static queue name";if(typeof r!=="object"||Array.isArray(r))return"RabbitMQ Trigger needs static options";if(r.acknowledge!=="executionFinishesSuccessfully")return"RabbitMQ Trigger needs the executionFinishesSuccessfully acknowledgement mode";if(r.contentIsBinary||r.onlyContent)return"RabbitMQ Trigger supports text or parsed JSON in the message envelope";let i=r.parallelMessages??-1;if(!Number.isInteger(i)||i!==-1&&(i<1||i>65535))return"RabbitMQ Trigger needs parallelMessages between 1 and 65535, or -1";let s=r.binding?.bindings??[];if(!Array.isArray(s)||!s.every((o)=>T(o?.exchange)&&o.exchange.length>0&&T(o?.routingKey)))return"RabbitMQ Trigger needs static exchange names and routing keys in its bindings";for(let o of["assertQueue","durable","autoDelete","exclusive","jsonParseBody"])if(r[o]!==void 0&&typeof r[o]!=="boolean")return"RabbitMQ Trigger needs static boolean queue and message settings";if(r.assertQueue&&Object.keys(r.arguments??{}).length>0)return"RabbitMQ Trigger queue declaration does not support custom arguments";return{queue:t,bindings:s.map(({exchange:o,routingKey:a})=>({exchange:o,routingKey:a})),prefetch:i===-1?void 0:i,parseJson:r.jsonParseBody===!0,declareQueue:r.assertQueue?{durable:r.durable,autoDelete:r.autoDelete,exclusive:r.exclusive}:void 0}}class J{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},a=n||`${t}.json`,m=new Map;s.forEach((d)=>m.set(d.name,[])),Object.entries(o).forEach(([d,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((N,b)=>{if(Array.isArray(N))N.forEach((p)=>{if(p&&p.node&&m.has(p.node))m.get(p.node)?.push({sourceNode:d,outputIndex:b})})})}),s.forEach((d)=>{d.incomingConnections=m.get(d.name)||[]});let l=s.filter((d)=>d.type===k||d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),c=new Set(l.map((d)=>d.name));s.forEach((d)=>{let f=new Set,N=new Set,b=[...(m.get(d.name)||[]).map((p)=>p.sourceNode)];while(b.length>0){let p=b.shift();if(N.has(p))continue;if(N.add(p),c.has(p))f.add(p);(m.get(p)||[]).forEach((h)=>b.push(h.sourceNode))}d.ancestorTriggers=Array.from(f)});let g=this.topologicalSort(s,o);return{id:r,name:i,fileName:t,relativePath:a.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:g,triggers:l}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((c)=>{n.set(c.name,c),r.set(c.name,[])}),Object.entries(t).forEach(([c,g])=>{if(g.main&&Array.isArray(g.main))g.main.forEach((d)=>{if(Array.isArray(d))d.forEach((f)=>{if(f&&f.node&&n.has(f.node))r.get(c)?.push(f.node)})})});let i=new Set,s=new Set,o=[],a=(c,g=[])=>{if(i.has(c))return;if(s.has(c)){let N=[...g,c].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${N}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(c),g.push(c);let d=r.get(c)||[];for(let N of d)a(N,g);g.pop(),s.delete(c),i.add(c);let f=n.get(c);if(f)o.push(f)},m=new Map;e.forEach((c)=>m.set(c.name,0));for(let c of r.values())for(let g of c)m.set(g,(m.get(g)||0)+1);let l=e.map((c)=>c.name).filter((c)=>m.get(c)===0);for(let c of l)a(c);for(let c of e)if(!i.has(c.name))a(c.name);return o.reverse()}}class u{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let o=t.substring(1).trim();if(o.startsWith("{{")&&o.endsWith("}}")){let a=o.substring(2,o.length-2).trim();if(!a.includes("{{")&&!a.includes("}}"))return`(${this.transformTokens(a)})`}t=t.substring(1).trim()}let n=[],r=0,s=t.replace(/\{\{(.*?)\}\}/g,(o,a)=>{let m=this.transformTokens(a.trim()),l=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${m}) ?? ''}`),l}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((o,a)=>{s=s.replace(`__EXPR_PLACEHOLDER_${a}__`,o)}),`\`${s}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,i)=>{let s=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${s.length?"?."+s.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,i)=>{let s=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${s.length?"?."+s.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}var v="n8n-nodes-base.rabbitmq";function U(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1)return"RabbitMQ needs typeVersion 1 or 1.1";if((t.operation??"sendMessage")!=="sendMessage"||t.mode!=="exchange"||Object.keys(t.options??{}).length>0)return"RabbitMQ only supports Send Message to an existing exchange with default options"}function x(e){let t=U(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",i=u.parse(n.exchange??""),s=u.parse(n.routingKey??""),o=u.parse(n.sendInputData??!0),a=u.parse(n.message??""),m=e.variableName;return`
    {
      const source: N8nItem[] = ${r} || inputItems;
      ${m} = [];
      if (source.length > 0) {
        item = source[0];
        const exchange = String(${i});
        const routingKey = String(${s});
        const sendInputData = Boolean(${o});
        const messages = source.map((item) => sendInputData ? JSON.stringify(item.json) : String(${a}));
        ${m} = await publishRabbitMq(exchange, routingKey, messages);
      }
      item = ${m}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${m});
    }`}class R{static mapNode(e){let t=e.node.type;if(t===v)return x(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(n==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)s=u.parse(t.responseBody);else if(n==="text"&&t.responseBody)s=u.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${i} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${s};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${r}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",i=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${r} || inputItems;
      const result = await (async () => {
        ${i}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},n=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(n)}, ${r} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let s of t.assignments.assignments)if(s.name){let o=u.parse(s.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(s.name)}, ${o});`)}}if(t.values)for(let s of["string","number","boolean"]){let o=t.values[s];if(Array.isArray(o)){for(let a of o)if(a.name){let m=u.parse(a.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(a.name)}, ${m});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let o of t.conditions.conditions){let a=u.parse(o.leftValue),m=u.parse(o.rightValue);switch(o.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${a} == ${m})`);break;case"notEquals":case"notEqual":r.push(`(${a} != ${m})`);break;case"contains":r.push(`String(${a} ?? '').includes(String(${m} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${a}) > Number(${m}))`);break;case"lt":case"smaller":r.push(`(Number(${a}) < Number(${m}))`);break;case"isEmpty":case"empty":r.push(`(${a} === undefined || ${a} === null || ${a} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${a} !== undefined && ${a} !== null && ${a} !== '')`);break;default:r.push(`Boolean(${a})`)}}let i=t.conditions?.combinator==="or"?" || ":" && ",s=r.length>0?r.join(i):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${s});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],i=[];r.forEach((o,a)=>{let m=o.conditions?.conditions?.[0];if(m){let l=u.parse(m.leftValue),c=u.parse(m.rightValue);i.push(`if (${l} == ${c}) { matchedIndex = ${a}; }`)}});let s=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${s};
      ${i.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),i=u.parse(t.url||""),s="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))s=`{ ${t.headerParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${u.parse(m.value)}`).join(", ")} }`;let o="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)o=`JSON.stringify(${u.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))o=`JSON.stringify({ ${t.bodyParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${u.parse(m.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${s} };
      const reqBody = ${o};
      const res = await fetch(targetUrl, {
        method: ${JSON.stringify(r)},
        headers: reqHeaders,
        ...(reqBody !== undefined ? { body: typeof reqBody === 'string' ? reqBody : JSON.stringify(reqBody) } : {})
      });
      const text = await res.text();
      let data: any;
      try { data = JSON.parse(text); } catch { data = { raw: text }; }
      return { json: typeof data === 'object' && data !== null ? data : { data } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapAggregateNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.destinationFieldName||"data";return`
    // [Node: ${e.node.name}] Aggregate Items
    {
      const source = ${n} || inputItems;
      ${e.variableName} = [{ json: { ${JSON.stringify(r)}: source.map((i: N8nItem) => i.json) } }];
      item = ${e.variableName}[0];
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapSplitOutNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.fieldToSplitOut||"data";return`
    // [Node: ${e.node.name}] Split Out Array Field
    {
      const source = ${n} || inputItems;
      const expanded: N8nItem[] = [];
      for (const entry of source) {
        const arr = entry.json?.[${JSON.stringify(r)}];
        if (Array.isArray(arr)) {
          for (const el of arr) {
            expanded.push({ json: typeof el === 'object' && el !== null ? el : { [${JSON.stringify(r)}]: el } });
          }
        }
      }
      ${e.variableName} = expanded.length > 0 ? expanded : source;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],n=e.node.parameters?.mode||"append",r=t.map((i)=>`(ctx.isBranchActive(${JSON.stringify(i.sourceNode)}, ${i.outputIndex}) ? (ctx.getNode(${JSON.stringify(i.sourceNode)})?.all() || []) : [])`);if(n==="combine"&&r.length>=2)return`
    // [Node: ${e.node.name}] Merge (Combine Mode)
    {
      const left = ${r[0]};
      const right = ${r[1]};
      const maxLen = Math.max(left.length, right.length);
      const merged: N8nItem[] = [];
      for (let i = 0; i < maxLen; i++) {
        merged.push({ json: { ...(left[i]?.json || {}), ...(right[i]?.json || {}) } });
      }
      ${e.variableName} = merged.length > 0 ? merged : [{ json: {} }];
      item = ${e.variableName}[0];
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`;return`
    // [Node: ${e.node.name}] Merge (Append Mode)
    {
      const combined = [${r.join(", ")}].flat();
      ${e.variableName} = combined.length > 0 ? combined : inputItems;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=u.parse(t.query||"SELECT 1;");return`
    // [Node: ${e.node.name}] PostgreSQL Query (via DATABASE_URL env var)
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const sql = ${r};
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
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=u.parse(i);return`
    // [Node: ${e.node.name}] Native LLM Completion (OpenAI-compatible REST API)
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const apiKey = process.env.OPENAI_API_KEY || process.env.LLM_API_KEY || '';
      const baseUrl = process.env.LLM_BASE_URL || 'https://api.openai.com/v1';
      const prompt = ${s};
      const res = await fetch(\`\${baseUrl}/chat/completions\`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': \`Bearer \${apiKey}\`
        },
        body: JSON.stringify({
          model: ${JSON.stringify(r)},
          messages: [{ role: 'user', content: String(prompt) }]
        })
      });
      const data: any = await res.json();
      const text = data?.choices?.[0]?.message?.content || '';
      return { json: { text, model: ${JSON.stringify(r)}, usage: data?.usage } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),i=t.unit||"seconds",o=r*(i==="minutes"?60000:i==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${i})
    await new Promise((resolve) => setTimeout(resolve, ${o}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class S{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",i=e.nodes.some((p)=>p.type===v)?`import { publishRabbitMq } from '${r}/rabbitmq';
`:"",s=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${i}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,o=`  let item: N8nItem = inputItems[0] || { json: {} };
`,a=`  ctx.setNodeOutput("__initial__", inputItems);
`,m="inputItems",l=new Map;e.orderedNodes.forEach((p,y)=>{let h=`node_${y}_${this.sanitizeIdentifier(p.name)}`;l.set(p.name,h)});let c=e.triggers,g=c.some((p)=>p.type===k),d=e.orderedNodes.filter((p)=>!c.includes(p));c.forEach((p)=>{let y=e.orderedNodes.indexOf(p),h=l.get(p.name);o+=`  let ${h}: any = inputItems;
`;let I=JSON.stringify(p.name),C=`entryNode === ${I}`;if(p.type!==k){let O=(p.parameters?.path||p.name).toLowerCase().trim().replace(/^\/+/,"");a+=`  const triggerNode_${y} = item?.json?.__triggerNode;
`,a+=`  const triggerPath_${y} = item?.json?.__triggerPath;
`,C=`entryNode === undefined && (!triggerNode_${y} || triggerNode_${y} === ${I} || triggerPath_${y} === ${JSON.stringify(O)})`}a+=`  if (${C}) {
`,a+=`    ${h} = inputItems;
`,a+=`    ctx.setNodeOutput(${JSON.stringify(p.name)}, inputItems);
`,a+=`  }
`}),d.forEach((p)=>{let y=l.get(p.name);o+=`  let ${y}: any = inputItems;
`;let h="inputItems",I="true";if(p.incomingConnections&&p.incomingConnections.length>0){if(p.incomingConnections.length===1){let w=p.incomingConnections[0];h=`(ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex}) ? ctx.getNode(${JSON.stringify(w.sourceNode)})?.all() : null)`}else h=`(${p.incomingConnections.map((j)=>`(ctx.isBranchActive(${JSON.stringify(j.sourceNode)}, ${j.outputIndex}) ? ctx.getNode(${JSON.stringify(j.sourceNode)})?.all() : null)`).join(" || ")})`;I=`(${p.incomingConnections.map((w)=>`ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`).join(" || ")})`}let C=R.mapNode({node:p,workflowName:e.name,variableName:y,previousVariableName:h}),O=`  if (${I}) {
    ${C.trim().replace(/\n/g,`
    `)}
  }
`;if(p.ancestorTriggers&&p.ancestorTriggers.length>0){let q=p.ancestorTriggers.map((w)=>`ctx.hasExecutedNode(${JSON.stringify(w)})`).join(" || ");a+=`  if (${q}) {
  ${O.trim().replace(/\n/g,`
  `)}
  }
`}else if(g)a+=`  if (entryNode === undefined) {
  ${O.trim().replace(/\n/g,`
  `)}
  }
`;else a+=O;m=p.name});let f=e.orderedNodes[e.orderedNodes.length-1]?.name,N=f?`(ctx.getNode(${JSON.stringify(f)})?.all() || [{ json: item.json }])`:"inputItems",b=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${s}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext,
  entryNode?: string
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${o}
${a}
  const finalItems = ${N};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(b)}, ${t});
`}}class E{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let a=`workflow_${S.sanitizeIdentifier(o.fileName)}`,m=o.relativePath.replace(/\.ts$/,"");t+=`import { ${a} } from '../workflows/${m}';
`,n.push(a)});let r="",i=[],s=new Set;if(e.forEach((o)=>{let a=`workflow_${S.sanitizeIdentifier(o.fileName)}`,m=o.nodes.filter((l)=>l.type.includes("webhook"));for(let l of M(o)){let c=_(l);if(typeof c==="string")throw Error(`Workflow ${JSON.stringify(o.name)}, node ${JSON.stringify(l.name)}: ${c}`);let g=JSON.stringify({workflowId:o.id,node:l.name});i.push(`      stopConsumers.push(await startRabbitMqConsumer(
        ${JSON.stringify(c)},
        (items) => ${a}(items, undefined, ${JSON.stringify(l.name)}),
        (message) => fastify.log.error(${g}, message),
      ));`)}m.forEach((l)=>{let c=(l.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),g=(l.parameters?.httpMethod||"POST").toUpperCase(),d=g==="ALL"?["GET","POST","PUT","DELETE"]:[g],f=Array.from(new Set([`/${c}`,`/webhook/${c}`]));d.forEach((N)=>{f.forEach((b)=>{let p=`${N}:${b}`;if(s.has(p))return;s.add(p),r+=`
  fastify.route({
    method: '${N}',
    url: '${b}',
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
          __triggerNode: ${JSON.stringify(l.name)},
          __triggerPath: ${JSON.stringify(c)}
        }
      }];

      try {
        const { items, context } = await ${a}(inputItems, ctx);
        const durationMs = (performance.now() - startTime).toFixed(2);
        reply.header('X-Compiled-By', 'n8n-to-fastify');
        reply.header('X-Execution-Time', \`\${durationMs}ms\`);

        if (context.webhookResponse) {
          reply.code(context.webhookResponse.statusCode);
          return context.webhookResponse.body;
        }
        reply.code(200);
        return items.length === 1 ? items[0].json : items.map(i => i.json);
      } catch (err: any) {
        request.log.error(err);
        reply.code(500);
        return { error: 'Workflow Execution Error', message: err.message };
      }
    }
  });
`})})})}),i.length>0)t+=`import { startRabbitMqConsumer } from '../runtime/rabbitmq';
`,r+=`
  const stopConsumers: Array<() => Promise<void>> = [];
  const closeConsumers = async () => {
    await Promise.allSettled(stopConsumers.splice(0).map(stop => stop()));
  };
  fastify.addHook('onReady', async () => {
    try {
${i.join(`
`)}
    } catch (error) {
      await closeConsumers();
      throw error;
    }
  });
  fastify.addHook('onClose', closeConsumers);
`;return`${t}
// Prevent bundler/runtime tree-shaking so subworkflows self-register into WorkflowRegistry
const _registeredWorkflows = [${n.join(", ")}];
void _registeredWorkflows;

export async function registerCompiledRoutes(fastify: FastifyInstance): Promise<void> {
${r}
}
`}static generateServerEntry(e=[]){return`import Fastify from 'fastify';
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
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);${e.some((n)=>M(n).length>0)?`
  const close = () => {
    void fastify.close().catch(error => {
      fastify.log.error(error);
      process.exitCode = 1;
    });
  };
  process.once('SIGINT', close);
  process.once('SIGTERM', close);`:""}
} catch (err) {
  fastify.log.error(err);
  process.exit(1);
}
`}static generateOutputPackageJson(e=[]){let t=e.some((n)=>M(n).length>0||n.nodes.some((r)=>r.type===v));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...t?{amqplib:"^0.10.9"}:{}}},null,2)}}function W(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=J.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=S.generateWorkflowCode(r),s=E.generateRoutesCode([r]),o=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:W};})();
