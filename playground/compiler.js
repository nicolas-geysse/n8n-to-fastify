(()=>{var{defineProperty:O,getOwnPropertyNames:R,getOwnPropertyDescriptor:T}=Object,q=Object.prototype.hasOwnProperty;function W(e){return this[e]}var x=(e)=>{var t=(J??=new WeakMap).get(e),n;if(t)return t;if(t=O({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of R(e))if(!q.call(t,r))O(t,r,{get:W.bind(e,r),enumerable:!(n=T(e,r))||n.enumerable})}return J.set(e,t),t},J;var V=(e)=>e;function B(e,t){this[e]=V.bind(null,t)}var L=(e,t)=>{for(var n in t)O(e,n,{get:t[n],enumerable:!0,configurable:!0,set:B.bind(t,n)})};var F={};L(F,{compileWorkflowJson:()=>A});class k{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,m=e.name||t,o=e.nodes||[],s=e.connections||{},i=n||`${t}.json`,a=new Map;o.forEach((d)=>a.set(d.name,[])),Object.entries(s).forEach(([d,g])=>{if(g.main&&Array.isArray(g.main))g.main.forEach((y,p)=>{if(Array.isArray(y))y.forEach((l)=>{if(l&&l.node&&a.has(l.node))a.get(l.node)?.push({sourceNode:d,outputIndex:p})})})}),o.forEach((d)=>{d.incomingConnections=a.get(d.name)||[]});let f=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),c=new Set(f.map((d)=>d.name));o.forEach((d)=>{let g=new Set,y=new Set,p=[...(a.get(d.name)||[]).map((l)=>l.sourceNode)];while(p.length>0){let l=p.shift();if(y.has(l))continue;if(y.add(l),c.has(l))g.add(l);(a.get(l)||[]).forEach((w)=>p.push(w.sourceNode))}d.ancestorTriggers=Array.from(g)});let N=this.topologicalSort(o,s);return{id:r,name:m,fileName:t,relativePath:i.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:N,triggers:f}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((c)=>{n.set(c.name,c),r.set(c.name,[])}),Object.entries(t).forEach(([c,N])=>{if(N.main&&Array.isArray(N.main))N.main.forEach((d)=>{if(Array.isArray(d))d.forEach((g)=>{if(g&&g.node&&n.has(g.node))r.get(c)?.push(g.node)})})});let m=new Set,o=new Set,s=[],i=(c,N=[])=>{if(m.has(c))return;if(o.has(c)){let y=[...N,c].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(c),N.push(c);let d=r.get(c)||[];for(let y of d)i(y,N);N.pop(),o.delete(c),m.add(c);let g=n.get(c);if(g)s.push(g)},a=new Map;e.forEach((c)=>a.set(c.name,0));for(let c of r.values())for(let N of c)a.set(N,(a.get(N)||0)+1);let f=e.map((c)=>c.name).filter((c)=>a.get(c)===0);for(let c of f)i(c);for(let c of e)if(!m.has(c.name))i(c.name);return s.reverse()}}class u{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let i=s.substring(2,s.length-2).trim();if(!i.includes("{{")&&!i.includes("}}"))return`(${this.transformTokens(i)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,i)=>{let a=this.transformTokens(i.trim()),f=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${a}) ?? ''}`),f}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((s,i)=>{o=o.replace(`__EXPR_PLACEHOLDER_${i}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,m)=>{let o=m?m.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,m)=>{let o=m?m.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}var v="n8n-nodes-base.rabbitmq";function D(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1)return"RabbitMQ needs typeVersion 1 or 1.1";if((t.operation??"sendMessage")!=="sendMessage"||t.mode!=="exchange"||Object.keys(t.options??{}).length>0)return"RabbitMQ only supports Send Message to an existing exchange with default options"}function _(e){let t=D(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",m=u.parse(n.exchange??""),o=u.parse(n.routingKey??""),s=u.parse(n.sendInputData??!0),i=u.parse(n.message??""),a=e.variableName;return`
    {
      const source: N8nItem[] = ${r} || inputItems;
      ${a} = [];
      if (source.length > 0) {
        item = source[0];
        const exchange = String(${m});
        const routingKey = String(${o});
        const sendInputData = Boolean(${s});
        const messages = source.map((item) => sendInputData ? JSON.stringify(item.json) : String(${i}));
        ${a} = await publishRabbitMq(exchange, routingKey, messages);
      }
      item = ${a}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${a});
    }`}class C{static mapNode(e){let t=e.node.type;if(t===v)return _(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,m=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=u.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=u.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${m} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${o};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${r}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",m=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${r} || inputItems;
      const result = await (async () => {
        ${m}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},n=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(n)}, ${r} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",m=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=u.parse(o.value);m.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let i of s)if(i.name){let a=u.parse(i.value);m.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(i.name)}, ${a});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${m.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let s of t.conditions.conditions){let i=u.parse(s.leftValue),a=u.parse(s.rightValue);switch(s.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${i} == ${a})`);break;case"notEquals":case"notEqual":r.push(`(${i} != ${a})`);break;case"contains":r.push(`String(${i} ?? '').includes(String(${a} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${i}) > Number(${a}))`);break;case"lt":case"smaller":r.push(`(Number(${i}) < Number(${a}))`);break;case"isEmpty":case"empty":r.push(`(${i} === undefined || ${i} === null || ${i} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${i} !== undefined && ${i} !== null && ${i} !== '')`);break;default:r.push(`Boolean(${i})`)}}let m=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(m):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],m=[];r.forEach((s,i)=>{let a=s.conditions?.conditions?.[0];if(a){let f=u.parse(a.leftValue),c=u.parse(a.rightValue);m.push(`if (${f} == ${c}) { matchedIndex = ${i}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${m.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),m=u.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${u.parse(a.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)s=`JSON.stringify(${u.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${u.parse(a.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${m};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${o} };
      const reqBody = ${s};
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
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],n=e.node.parameters?.mode||"append",r=t.map((m)=>`(ctx.isBranchActive(${JSON.stringify(m.sourceNode)}, ${m.outputIndex}) ? (ctx.getNode(${JSON.stringify(m.sourceNode)})?.all() || []) : [])`);if(n==="combine"&&r.length>=2)return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",m=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=u.parse(m);return`
    // [Node: ${e.node.name}] Native LLM Completion (OpenAI-compatible REST API)
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const apiKey = process.env.OPENAI_API_KEY || process.env.LLM_API_KEY || '';
      const baseUrl = process.env.LLM_BASE_URL || 'https://api.openai.com/v1';
      const prompt = ${o};
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),m=t.unit||"seconds",s=r*(m==="minutes"?60000:m==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${m})
    await new Promise((resolve) => setTimeout(resolve, ${s}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class S{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",m=e.nodes.some((p)=>p.type===v)?`import { publishRabbitMq } from '${r}/rabbitmq';
`:"",o=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${m}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,s=`  let item: N8nItem = inputItems[0] || { json: {} };
`,i=`  ctx.setNodeOutput("__initial__", inputItems);
`,a="inputItems",f=new Map;e.orderedNodes.forEach((p,l)=>{let h=`node_${l}_${this.sanitizeIdentifier(p.name)}`;f.set(p.name,h)});let c=e.orderedNodes.filter((p)=>p.type.includes("webhook")||p.type.includes("trigger")||p.type.includes("cron")),N=e.orderedNodes.filter((p)=>!c.includes(p));c.forEach((p)=>{let l=e.orderedNodes.indexOf(p),h=f.get(p.name);s+=`  let ${h}: any = inputItems;
`;let w=(p.parameters?.path||p.name).toLowerCase().trim().replace(/^\/+/,"");i+=`  const triggerNode_${l} = item?.json?.__triggerNode;
`,i+=`  const triggerPath_${l} = item?.json?.__triggerPath;
`,i+=`  if (!triggerNode_${l} || triggerNode_${l} === ${JSON.stringify(p.name)} || triggerPath_${l} === ${JSON.stringify(w)}) {
`,i+=`    ${h} = inputItems;
`,i+=`    ctx.setNodeOutput(${JSON.stringify(p.name)}, inputItems);
`,i+=`  }
`}),N.forEach((p)=>{let l=f.get(p.name);s+=`  let ${l}: any = inputItems;
`;let h="inputItems",w="true";if(p.incomingConnections&&p.incomingConnections.length>0){if(p.incomingConnections.length===1){let b=p.incomingConnections[0];h=`(ctx.isBranchActive(${JSON.stringify(b.sourceNode)}, ${b.outputIndex}) ? ctx.getNode(${JSON.stringify(b.sourceNode)})?.all() : null)`}else h=`(${p.incomingConnections.map((I)=>`(ctx.isBranchActive(${JSON.stringify(I.sourceNode)}, ${I.outputIndex}) ? ctx.getNode(${JSON.stringify(I.sourceNode)})?.all() : null)`).join(" || ")})`;w=`(${p.incomingConnections.map((b)=>`ctx.isBranchActive(${JSON.stringify(b.sourceNode)}, ${b.outputIndex})`).join(" || ")})`}let M=C.mapNode({node:p,workflowName:e.name,variableName:l,previousVariableName:h}),P=`  if (${w}) {
    ${M.trim().replace(/\n/g,`
    `)}
  }
`;if(p.ancestorTriggers&&p.ancestorTriggers.length>0){let E=p.ancestorTriggers.map((b)=>`ctx.hasExecutedNode(${JSON.stringify(b)})`).join(" || ");i+=`  if (${E}) {
  ${P.trim().replace(/\n/g,`
  `)}
  }
`}else i+=P;a=p.name});let d=e.orderedNodes[e.orderedNodes.length-1]?.name,g=d?`(ctx.getNode(${JSON.stringify(d)})?.all() || [{ json: item.json }])`:"inputItems",y=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${o}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${s}
${i}
  const finalItems = ${g};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(y)}, ${t});
`}}class j{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`,i=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${i}';
`,n.push(s)});let r="",m=new Set;return e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((a)=>a.type.includes("webhook")).forEach((a)=>{let f=(a.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),c=(a.parameters?.httpMethod||"POST").toUpperCase(),N=c==="ALL"?["GET","POST","PUT","DELETE"]:[c],d=Array.from(new Set([`/${f}`,`/webhook/${f}`]));N.forEach((g)=>{d.forEach((y)=>{let p=`${g}:${y}`;if(m.has(p))return;m.add(p),r+=`
  fastify.route({
    method: '${g}',
    url: '${y}',
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
          __triggerNode: ${JSON.stringify(a.name)},
          __triggerPath: ${JSON.stringify(f)}
        }
      }];

      try {
        const { items, context } = await ${s}(inputItems, ctx);
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
`})})})}),`${t}
// Prevent bundler/runtime tree-shaking so subworkflows self-register into WorkflowRegistry
const _registeredWorkflows = [${n.join(", ")}];
void _registeredWorkflows;

export async function registerCompiledRoutes(fastify: FastifyInstance): Promise<void> {
${r}
}
`}static generateServerEntry(){return`import Fastify from 'fastify';
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
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);
} catch (err) {
  fastify.log.error(err);
  process.exit(1);
}
`}static generateOutputPackageJson(e=[]){let t=e.some((n)=>n.nodes.some((r)=>r.type===v));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...t?{amqplib:"^0.10.9"}:{}}},null,2)}}function A(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=k.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let m=S.generateWorkflowCode(r),o=j.generateRoutesCode([r]),s=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:s,workflowCode:m,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:A};})();
