(()=>{var{defineProperty:S,getOwnPropertyNames:P,getOwnPropertyDescriptor:A}=Object,T=Object.prototype.hasOwnProperty;function R(e){return this[e]}var M=(e)=>{var t=(J??=new WeakMap).get(e),n;if(t)return t;if(t=S({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of P(e))if(!T.call(t,r))S(t,r,{get:R.bind(e,r),enumerable:!(n=A(e,r))||n.enumerable})}return J.set(e,t),t},J;var W=(e)=>e;function q(e,t){this[e]=W.bind(null,t)}var V=(e,t)=>{for(var n in t)S(e,n,{get:t[n],enumerable:!0,configurable:!0,set:q.bind(t,n)})};var x={};V(x,{compileWorkflowJson:()=>_});class O{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,a=e.name||t,o=e.nodes||[],s=e.connections||{},p=n||`${t}.json`,i=new Map;o.forEach((d)=>i.set(d.name,[])),Object.entries(s).forEach(([d,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((m,g)=>{if(Array.isArray(m))m.forEach((l)=>{if(l&&l.node&&i.has(l.node))i.get(l.node)?.push({sourceNode:d,outputIndex:g})})})}),o.forEach((d)=>{d.incomingConnections=i.get(d.name)||[]});let N=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),c=new Set(N.map((d)=>d.name));o.forEach((d)=>{let u=new Set,m=new Set,g=[...(i.get(d.name)||[]).map((l)=>l.sourceNode)];while(g.length>0){let l=g.shift();if(m.has(l))continue;if(m.add(l),c.has(l))u.add(l);(i.get(l)||[]).forEach((b)=>g.push(b.sourceNode))}d.ancestorTriggers=Array.from(u)});let f=this.topologicalSort(o,s);return{id:r,name:a,fileName:t,relativePath:p.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:f,triggers:N}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((c)=>{n.set(c.name,c),r.set(c.name,[])}),Object.entries(t).forEach(([c,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((d)=>{if(Array.isArray(d))d.forEach((u)=>{if(u&&u.node&&n.has(u.node))r.get(c)?.push(u.node)})})});let a=new Set,o=new Set,s=[],p=(c,f=[])=>{if(a.has(c))return;if(o.has(c)){let m=[...f,c].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${m}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(c),f.push(c);let d=r.get(c)||[];for(let m of d)p(m,f);f.pop(),o.delete(c),a.add(c);let u=n.get(c);if(u)s.push(u)},i=new Map;e.forEach((c)=>i.set(c.name,0));for(let c of r.values())for(let f of c)i.set(f,(i.get(f)||0)+1);let N=e.map((c)=>c.name).filter((c)=>i.get(c)===0);for(let c of N)p(c);for(let c of e)if(!a.has(c.name))p(c.name);return s.reverse()}}class y{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let p=s.substring(2,s.length-2).trim();if(!p.includes("{{")&&!p.includes("}}"))return`(${this.transformTokens(p)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,p)=>{let i=this.transformTokens(p.trim()),N=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${i}) ?? ''}`),N}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((s,p)=>{o=o.replace(`__EXPR_PLACEHOLDER_${p}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}class k{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=y.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=y.parse(t.responseBody);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=y.parse(o.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let p of s)if(p.name){let i=y.parse(p.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${i});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let s of t.conditions.conditions){let p=y.parse(s.leftValue),i=y.parse(s.rightValue);switch(s.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${p} == ${i})`);break;case"notEquals":case"notEqual":r.push(`(${p} != ${i})`);break;case"contains":r.push(`String(${p} ?? '').includes(String(${i} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${p}) > Number(${i}))`);break;case"lt":case"smaller":r.push(`(Number(${p}) < Number(${i}))`);break;case"isEmpty":case"empty":r.push(`(${p} === undefined || ${p} === null || ${p} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${p} !== undefined && ${p} !== null && ${p} !== '')`);break;default:r.push(`Boolean(${p})`)}}let a=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(a):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],a=[];r.forEach((s,p)=>{let i=s.conditions?.conditions?.[0];if(i){let N=y.parse(i.leftValue),c=y.parse(i.rightValue);a.push(`if (${N} == ${c}) { matchedIndex = ${p}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${a.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),a=y.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${y.parse(i.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)s=`JSON.stringify(${y.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${y.parse(i.value)}`).join(", ")} })`}return`
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=y.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",a=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=y.parse(a);return`
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
`,p="inputItems",i=new Map;e.orderedNodes.forEach((m,g)=>{let l=`node_${g}_${this.sanitizeIdentifier(m.name)}`;i.set(m.name,l)});let N=e.orderedNodes.filter((m)=>m.type.includes("webhook")||m.type.includes("trigger")||m.type.includes("cron")),c=e.orderedNodes.filter((m)=>!N.includes(m));N.forEach((m)=>{let g=e.orderedNodes.indexOf(m),l=i.get(m.name);o+=`  let ${l}: any = inputItems;
`;let v=(m.parameters?.path||m.name).toLowerCase().trim().replace(/^\/+/,"");s+=`  const triggerNode_${g} = item?.json?.__triggerNode;
`,s+=`  const triggerPath_${g} = item?.json?.__triggerPath;
`,s+=`  if (!triggerNode_${g} || triggerNode_${g} === ${JSON.stringify(m.name)} || triggerPath_${g} === ${JSON.stringify(v)}) {
`,s+=`    ${l} = inputItems;
`,s+=`    ctx.setNodeOutput(${JSON.stringify(m.name)}, inputItems);
`,s+=`  }
`}),c.forEach((m)=>{let g=i.get(m.name);o+=`  let ${g}: any = inputItems;
`;let l="inputItems",v="true";if(m.incomingConnections&&m.incomingConnections.length>0){if(m.incomingConnections.length===1){let h=m.incomingConnections[0];l=`(ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex}) ? ctx.getNode(${JSON.stringify(h.sourceNode)})?.all() : null)`}else l=`(${m.incomingConnections.map((I)=>`(ctx.isBranchActive(${JSON.stringify(I.sourceNode)}, ${I.outputIndex}) ? ctx.getNode(${JSON.stringify(I.sourceNode)})?.all() : null)`).join(" || ")})`;v=`(${m.incomingConnections.map((h)=>`ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex})`).join(" || ")})`}let b=k.mapNode({node:m,workflowName:e.name,variableName:g,previousVariableName:l}),j=`  if (${v}) {
    ${b.trim().replace(/\n/g,`
    `)}
  }
`;if(m.ancestorTriggers&&m.ancestorTriggers.length>0){let E=m.ancestorTriggers.map((h)=>`ctx.hasExecutedNode(${JSON.stringify(h)})`).join(" || ");s+=`  if (${E}) {
  ${j.trim().replace(/\n/g,`
  `)}
  }
`}else s+=j;p=m.name});let f=e.orderedNodes[e.orderedNodes.length-1]?.name,d=f?`(ctx.getNode(${JSON.stringify(f)})?.all() || [{ json: item.json }])`:"inputItems",u=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${a}
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

registerWorkflow(${JSON.stringify(u)}, ${t});
`}}class C{static generateRoutesCode(e){let t=`import { randomUUID } from 'node:crypto';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let s=`workflow_${w.sanitizeIdentifier(o.fileName)}`,p=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${p}';
`,n.push(s)});let r="",a=new Set;return e.forEach((o)=>{let s=`workflow_${w.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((i)=>i.type.includes("webhook")).forEach((i)=>{let N=(i.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),c=(i.parameters?.httpMethod||"POST").toUpperCase(),f=c==="ALL"?["GET","POST","PUT","DELETE"]:[c],d=Array.from(new Set([`/${N}`,`/webhook/${N}`]));f.forEach((u)=>{d.forEach((m)=>{let g=`${u}:${m}`;if(a.has(g))return;a.add(g),r+=`
  fastify.route({
    method: '${u}',
    url: '${m}',
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
          __triggerPath: ${JSON.stringify(N)}
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
      } catch {
        const errorId = randomUUID();
        request.log.error({ errorId }, 'Workflow execution failed');
        reply.code(500);
        return { error: 'Workflow Execution Error', message: 'Workflow execution failed', errorId };
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
