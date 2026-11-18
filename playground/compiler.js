(()=>{var{defineProperty:O,getOwnPropertyNames:M,getOwnPropertyDescriptor:R}=Object,W=Object.prototype.hasOwnProperty;function V(e){return this[e]}var q=(e)=>{var t=(J??=new WeakMap).get(e),n;if(t)return t;if(t=O({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of M(e))if(!W.call(t,r))O(t,r,{get:V.bind(e,r),enumerable:!(n=R(e,r))||n.enumerable})}return J.set(e,t),t},J;var x=(e)=>e;function B(e,t){this[e]=x.bind(null,t)}var L=(e,t)=>{for(var n in t)O(e,n,{get:t[n],enumerable:!0,configurable:!0,set:B.bind(t,n)})};var D={};L(D,{compileWorkflowJson:()=>_});class k{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,s=e.name||t,o=e.nodes||[],i=e.connections||{},a=n||`${t}.json`,p=new Map;o.forEach((d)=>p.set(d.name,[])),Object.entries(i).forEach(([d,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((y,m)=>{if(Array.isArray(y))y.forEach((l)=>{if(l&&l.node&&p.has(l.node))p.get(l.node)?.push({sourceNode:d,outputIndex:m})})})}),o.forEach((d)=>{d.incomingConnections=p.get(d.name)||[]});let f=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),c=new Set(f.map((d)=>d.name));o.forEach((d)=>{let u=new Set,y=new Set,m=[...(p.get(d.name)||[]).map((l)=>l.sourceNode)];while(m.length>0){let l=m.shift();if(y.has(l))continue;if(y.add(l),c.has(l))u.add(l);(p.get(l)||[]).forEach((b)=>m.push(b.sourceNode))}d.ancestorTriggers=Array.from(u)});let N=this.topologicalSort(o,i);return{id:r,name:s,fileName:t,relativePath:a.replace(/\.json$/,".ts"),nodes:o,connections:i,orderedNodes:N,triggers:f}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((c)=>{n.set(c.name,c),r.set(c.name,[])}),Object.entries(t).forEach(([c,N])=>{if(N.main&&Array.isArray(N.main))N.main.forEach((d)=>{if(Array.isArray(d))d.forEach((u)=>{if(u&&u.node&&n.has(u.node))r.get(c)?.push(u.node)})})});let s=new Set,o=new Set,i=[],a=(c,N=[])=>{if(s.has(c))return;if(o.has(c)){let y=[...N,c].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(c),N.push(c);let d=r.get(c)||[];for(let y of d)a(y,N);N.pop(),o.delete(c),s.add(c);let u=n.get(c);if(u)i.push(u)},p=new Map;e.forEach((c)=>p.set(c.name,0));for(let c of r.values())for(let N of c)p.set(N,(p.get(N)||0)+1);let f=e.map((c)=>c.name).filter((c)=>p.get(c)===0);for(let c of f)a(c);for(let c of e)if(!s.has(c.name))a(c.name);return i.reverse()}}class g{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let i=t.substring(1).trim();if(i.startsWith("{{")&&i.endsWith("}}")){let a=i.substring(2,i.length-2).trim();if(!a.includes("{{")&&!a.includes("}}"))return`(${this.transformTokens(a)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(i,a)=>{let p=this.transformTokens(a.trim()),f=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${p}) ?? ''}`),f}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((i,a)=>{o=o.replace(`__EXPR_PLACEHOLDER_${a}__`,i)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}var S="n8n-nodes-base.stopAndError";function F(e){if((e.typeVersion??1)!==1)return"Stop and Error supports node version 1";let t=e.parameters?.errorType??"errorMessage";if(t!=="errorMessage"&&t!=="errorObject")return"Stop and Error supports Error Message and Error Object"}function P(e){let t=F(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=n.errorType??"errorMessage",s=n[r]??"",o=g.parse(typeof s==="string"&&!s.includes("{{")?s.replace(/^=/,""):s);return`
    {
      const source: N8nItem[] = ${e.previousVariableName??"inputItems"} || inputItems;
      if (source.length > 0) {
        const inputItems = source;
        const item = inputItems[0];
        throw createWorkflowStopError(${JSON.stringify(r)}, ${o});
      }
    }`}class E{static mapNode(e){let t=e.node.type;if(t===S)return P(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,s=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=g.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=g.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${s} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${o};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${r}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",s=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,i)=>`(ctx.getNode(${JSON.stringify(i)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,i)=>`(ctx.getNode(${JSON.stringify(i)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,i)=>`(ctx.getNode(${JSON.stringify(i)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${r} || inputItems;
      const result = await (async () => {
        ${s}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},n=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(n)}, ${r} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",s=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let i=g.parse(o.value);s.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${i});`)}}if(t.values)for(let o of["string","number","boolean"]){let i=t.values[o];if(Array.isArray(i)){for(let a of i)if(a.name){let p=g.parse(a.value);s.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(a.name)}, ${p});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${s.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let i of t.conditions.conditions){let a=g.parse(i.leftValue),p=g.parse(i.rightValue);switch(i.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${a} == ${p})`);break;case"notEquals":case"notEqual":r.push(`(${a} != ${p})`);break;case"contains":r.push(`String(${a} ?? '').includes(String(${p} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${a}) > Number(${p}))`);break;case"lt":case"smaller":r.push(`(Number(${a}) < Number(${p}))`);break;case"isEmpty":case"empty":r.push(`(${a} === undefined || ${a} === null || ${a} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${a} !== undefined && ${a} !== null && ${a} !== '')`);break;default:r.push(`Boolean(${a})`)}}let s=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(s):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],s=[];r.forEach((i,a)=>{let p=i.conditions?.conditions?.[0];if(p){let f=g.parse(p.leftValue),c=g.parse(p.rightValue);s.push(`if (${f} == ${c}) { matchedIndex = ${a}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${s.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),s=g.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((p)=>p.name).map((p)=>`${JSON.stringify(p.name)}: ${g.parse(p.value)}`).join(", ")} }`;let i="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)i=`JSON.stringify(${g.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))i=`JSON.stringify({ ${t.bodyParameters.parameters.filter((p)=>p.name).map((p)=>`${JSON.stringify(p.name)}: ${g.parse(p.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${s};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${o} };
      const reqBody = ${i};
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
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],n=e.node.parameters?.mode||"append",r=t.map((s)=>`(ctx.isBranchActive(${JSON.stringify(s.sourceNode)}, ${s.outputIndex}) ? (ctx.getNode(${JSON.stringify(s.sourceNode)})?.all() || []) : [])`);if(n==="combine"&&r.length>=2)return`
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=g.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",s=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=g.parse(s);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),s=t.unit||"seconds",i=r*(s==="minutes"?60000:s==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${s})
    await new Promise((resolve) => setTimeout(resolve, ${i}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class v{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",s=e.nodes.some((m)=>m.type===S)?`import { createWorkflowStopError } from '${r}/stop-and-error';
`:"",o=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${s}

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,i=`  let item: N8nItem = inputItems[0] || { json: {} };
`,a=`  ctx.setNodeOutput("__initial__", inputItems);
`,p="inputItems",f=new Map;e.orderedNodes.forEach((m,l)=>{let h=`node_${l}_${this.sanitizeIdentifier(m.name)}`;f.set(m.name,h)});let c=e.orderedNodes.filter((m)=>m.type.includes("webhook")||m.type.includes("trigger")||m.type.includes("cron")),N=e.orderedNodes.filter((m)=>!c.includes(m));c.forEach((m)=>{let l=e.orderedNodes.indexOf(m),h=f.get(m.name);i+=`  let ${h}: any = inputItems;
`;let b=(m.parameters?.path||m.name).toLowerCase().trim().replace(/^\/+/,"");a+=`  const triggerNode_${l} = item?.json?.__triggerNode;
`,a+=`  const triggerPath_${l} = item?.json?.__triggerPath;
`,a+=`  if (!triggerNode_${l} || triggerNode_${l} === ${JSON.stringify(m.name)} || triggerPath_${l} === ${JSON.stringify(b)}) {
`,a+=`    ${h} = inputItems;
`,a+=`    ctx.setNodeOutput(${JSON.stringify(m.name)}, inputItems);
`,a+=`  }
`}),N.forEach((m)=>{let l=f.get(m.name);i+=`  let ${l}: any = inputItems;
`;let h="inputItems",b="true";if(m.incomingConnections&&m.incomingConnections.length>0){if(m.incomingConnections.length===1){let w=m.incomingConnections[0];h=`(ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex}) ? ctx.getNode(${JSON.stringify(w.sourceNode)})?.all() : null)`}else h=`(${m.incomingConnections.map((I)=>`(ctx.isBranchActive(${JSON.stringify(I.sourceNode)}, ${I.outputIndex}) ? ctx.getNode(${JSON.stringify(I.sourceNode)})?.all() : null)`).join(" || ")})`;b=`(${m.incomingConnections.map((w)=>`ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`).join(" || ")})`}let T=E.mapNode({node:m,workflowName:e.name,variableName:l,previousVariableName:h}),j=`  if (${b}) {
    ${T.trim().replace(/\n/g,`
    `)}
  }
`;if(m.ancestorTriggers&&m.ancestorTriggers.length>0){let A=m.ancestorTriggers.map((w)=>`ctx.hasExecutedNode(${JSON.stringify(w)})`).join(" || ");a+=`  if (${A}) {
  ${j.trim().replace(/\n/g,`
  `)}
  }
`}else a+=j;p=m.name});let d=e.orderedNodes[e.orderedNodes.length-1]?.name,u=d?`(ctx.getNode(${JSON.stringify(d)})?.all() || [{ json: item.json }])`:"inputItems",y=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${o}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${i}
${a}
  const finalItems = ${u};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(y)}, ${t});
`}}class C{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let i=`workflow_${v.sanitizeIdentifier(o.fileName)}`,a=o.relativePath.replace(/\.ts$/,"");t+=`import { ${i} } from '../workflows/${a}';
`,n.push(i)});let r="",s=new Set;return e.forEach((o)=>{let i=`workflow_${v.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((p)=>p.type.includes("webhook")).forEach((p)=>{let f=(p.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),c=(p.parameters?.httpMethod||"POST").toUpperCase(),N=c==="ALL"?["GET","POST","PUT","DELETE"]:[c],d=Array.from(new Set([`/${f}`,`/webhook/${f}`]));N.forEach((u)=>{d.forEach((y)=>{let m=`${u}:${y}`;if(s.has(m))return;s.add(m),r+=`
  fastify.route({
    method: '${u}',
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
          __triggerNode: ${JSON.stringify(p.name)},
          __triggerPath: ${JSON.stringify(f)}
        }
      }];

      try {
        const { items, context } = await ${i}(inputItems, ctx);
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function _(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=k.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let s=v.generateWorkflowCode(r),o=C.generateRoutesCode([r]),i=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:i,workflowCode:s,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:_};})();
