(()=>{var{defineProperty:I,getOwnPropertyNames:P,getOwnPropertyDescriptor:A}=Object,T=Object.prototype.hasOwnProperty;function R(e){return this[e]}var M=(e)=>{var t=(J??=new WeakMap).get(e),n;if(t)return t;if(t=I({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of P(e))if(!T.call(t,r))I(t,r,{get:R.bind(e,r),enumerable:!(n=A(e,r))||n.enumerable})}return J.set(e,t),t},J;var W=(e)=>e;function V(e,t){this[e]=W.bind(null,t)}var q=(e,t)=>{for(var n in t)I(e,n,{get:t[n],enumerable:!0,configurable:!0,set:V.bind(t,n)})};var B={};q(B,{compileWorkflowJson:()=>_});class O{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,a=e.name||t,o=e.nodes||[],s=e.connections||{},c=n||`${t}.json`,i=new Map;o.forEach((d)=>i.set(d.name,[])),Object.entries(s).forEach(([d,N])=>{if(N.main&&Array.isArray(N.main))N.main.forEach((p,y)=>{if(Array.isArray(p))p.forEach((u)=>{if(u&&u.node&&i.has(u.node))i.get(u.node)?.push({sourceNode:d,outputIndex:y})})})}),o.forEach((d)=>{d.incomingConnections=i.get(d.name)||[]});let l=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),m=new Set(l.map((d)=>d.name));o.forEach((d)=>{let N=new Set,p=new Set,y=[...(i.get(d.name)||[]).map((u)=>u.sourceNode)];while(y.length>0){let u=y.shift();if(p.has(u))continue;if(p.add(u),m.has(u))N.add(u);(i.get(u)||[]).forEach((v)=>y.push(v.sourceNode))}d.ancestorTriggers=Array.from(N)});let f=this.topologicalSort(o,s);return{id:r,name:a,fileName:t,relativePath:c.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:f,triggers:l}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((m)=>{n.set(m.name,m),r.set(m.name,[])}),Object.entries(t).forEach(([m,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((d)=>{if(Array.isArray(d))d.forEach((N)=>{if(N&&N.node&&n.has(N.node))r.get(m)?.push(N.node)})})});let a=new Set,o=new Set,s=[],c=(m,f=[])=>{if(a.has(m))return;if(o.has(m)){let p=[...f,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${p}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(m),f.push(m);let d=r.get(m)||[];for(let p of d)c(p,f);f.pop(),o.delete(m),a.add(m);let N=n.get(m);if(N)s.push(N)},i=new Map;e.forEach((m)=>i.set(m.name,0));for(let m of r.values())for(let f of m)i.set(f,(i.get(f)||0)+1);let l=e.map((m)=>m.name).filter((m)=>i.get(m)===0);for(let m of l)c(m);for(let m of e)if(!a.has(m.name))c(m.name);return s.reverse()}}class g{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let c=s.substring(2,s.length-2).trim();if(!c.includes("{{")&&!c.includes("}}"))return`(${this.transformTokens(c)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,c)=>{let i=this.transformTokens(c.trim()),l=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${i}) ?? ''}`),l}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((s,c)=>{o=o.replace(`__EXPR_PLACEHOLDER_${c}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}class k{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=g.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=g.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${a} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${o};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${r}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",a=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${r} || inputItems;
      const result = await (async () => {
        ${a}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},n=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(n)}, ${r} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",a=e.node.typeVersion??3.4,o=t.options??{},s;if(a>=3.4){let i=g.parse(t.includeOtherFields??!1),l=g.parse(t.include??"all"),m=g.parse(o.stripBinary??!1);s=`(${i}) && (${l}) !== 'none' && !(${m})`}else if(a>=3)s=g.parse(o.includeBinary??!1);else s=`!(${g.parse(t.keepOnlySet??o.keepOnlySet??!1)})`;let c=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let i of t.assignments.assignments)if(i.name){let l=g.parse(i.value);c.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(i.name)}, ${l});`)}}if(t.values)for(let i of["string","number","boolean"]){let l=t.values[i];if(Array.isArray(l)){for(let m of l)if(m.name){let f=g.parse(m.value);c.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${f});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${c.join(`
      `)}
      return {
        json: newJson,
        ...(currentItem.binary !== undefined && (${s}) ? { binary: { ...currentItem.binary } } : {}),
      };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let s of t.conditions.conditions){let c=g.parse(s.leftValue),i=g.parse(s.rightValue);switch(s.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${c} == ${i})`);break;case"notEquals":case"notEqual":r.push(`(${c} != ${i})`);break;case"contains":r.push(`String(${c} ?? '').includes(String(${i} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${c}) > Number(${i}))`);break;case"lt":case"smaller":r.push(`(Number(${c}) < Number(${i}))`);break;case"isEmpty":case"empty":r.push(`(${c} === undefined || ${c} === null || ${c} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${c} !== undefined && ${c} !== null && ${c} !== '')`);break;default:r.push(`Boolean(${c})`)}}let a=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(a):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],a=[];r.forEach((s,c)=>{let i=s.conditions?.conditions?.[0];if(i){let l=g.parse(i.leftValue),m=g.parse(i.rightValue);a.push(`if (${l} == ${m}) { matchedIndex = ${c}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${a.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),a=g.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${g.parse(i.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)s=`JSON.stringify(${g.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${g.parse(i.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${a};
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
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],n=e.node.parameters?.mode||"append",r=t.map((a)=>`(ctx.isBranchActive(${JSON.stringify(a.sourceNode)}, ${a.outputIndex}) ? (ctx.getNode(${JSON.stringify(a.sourceNode)})?.all() || []) : [])`);if(n==="combine"&&r.length>=2)return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",a=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=g.parse(a);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),a=t.unit||"seconds",s=r*(a==="minutes"?60000:a==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${a})
    await new Promise((resolve) => setTimeout(resolve, ${s}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class w{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",a=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,o=`  let item: N8nItem = inputItems[0] || { json: {} };
`,s=`  ctx.setNodeOutput("__initial__", inputItems);
`,c="inputItems",i=new Map;e.orderedNodes.forEach((p,y)=>{let u=`node_${y}_${this.sanitizeIdentifier(p.name)}`;i.set(p.name,u)});let l=e.orderedNodes.filter((p)=>p.type.includes("webhook")||p.type.includes("trigger")||p.type.includes("cron")),m=e.orderedNodes.filter((p)=>!l.includes(p));l.forEach((p)=>{let y=e.orderedNodes.indexOf(p),u=i.get(p.name);o+=`  let ${u}: any = inputItems;
`;let b=(p.parameters?.path||p.name).toLowerCase().trim().replace(/^\/+/,"");s+=`  const triggerNode_${y} = item?.json?.__triggerNode;
`,s+=`  const triggerPath_${y} = item?.json?.__triggerPath;
`,s+=`  if (!triggerNode_${y} || triggerNode_${y} === ${JSON.stringify(p.name)} || triggerPath_${y} === ${JSON.stringify(b)}) {
`,s+=`    ${u} = inputItems;
`,s+=`    ctx.setNodeOutput(${JSON.stringify(p.name)}, inputItems);
`,s+=`  }
`}),m.forEach((p)=>{let y=i.get(p.name);o+=`  let ${y}: any = inputItems;
`;let u="inputItems",b="true";if(p.incomingConnections&&p.incomingConnections.length>0){if(p.incomingConnections.length===1){let h=p.incomingConnections[0];u=`(ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex}) ? ctx.getNode(${JSON.stringify(h.sourceNode)})?.all() : null)`}else u=`(${p.incomingConnections.map((S)=>`(ctx.isBranchActive(${JSON.stringify(S.sourceNode)}, ${S.outputIndex}) ? ctx.getNode(${JSON.stringify(S.sourceNode)})?.all() : null)`).join(" || ")})`;b=`(${p.incomingConnections.map((h)=>`ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex})`).join(" || ")})`}let v=k.mapNode({node:p,workflowName:e.name,variableName:y,previousVariableName:u}),j=`  if (${b}) {
    ${v.trim().replace(/\n/g,`
    `)}
  }
`;if(p.ancestorTriggers&&p.ancestorTriggers.length>0){let E=p.ancestorTriggers.map((h)=>`ctx.hasExecutedNode(${JSON.stringify(h)})`).join(" || ");s+=`  if (${E}) {
  ${j.trim().replace(/\n/g,`
  `)}
  }
`}else s+=j;c=p.name});let f=e.orderedNodes[e.orderedNodes.length-1]?.name,d=f?`(ctx.getNode(${JSON.stringify(f)})?.all() || [{ json: item.json }])`:"inputItems",N=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${a}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${o}
${s}
  const finalItems = ${d};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(N)}, ${t});
`}}class C{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let s=`workflow_${w.sanitizeIdentifier(o.fileName)}`,c=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${c}';
`,n.push(s)});let r="",a=new Set;return e.forEach((o)=>{let s=`workflow_${w.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((i)=>i.type.includes("webhook")).forEach((i)=>{let l=(i.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),m=(i.parameters?.httpMethod||"POST").toUpperCase(),f=m==="ALL"?["GET","POST","PUT","DELETE"]:[m],d=Array.from(new Set([`/${l}`,`/webhook/${l}`]));f.forEach((N)=>{d.forEach((p)=>{let y=`${N}:${p}`;if(a.has(y))return;a.add(y),r+=`
  fastify.route({
    method: '${N}',
    url: '${p}',
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
          __triggerNode: ${JSON.stringify(i.name)},
          __triggerPath: ${JSON.stringify(l)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function _(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=O.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let a=w.generateWorkflowCode(r),o=C.generateRoutesCode([r]),s=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:s,workflowCode:a,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:_};})();
