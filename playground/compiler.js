(()=>{var{defineProperty:I,getOwnPropertyNames:_,getOwnPropertyDescriptor:P}=Object,A=Object.prototype.hasOwnProperty;function T(e){return this[e]}var R=(e)=>{var t=(j??=new WeakMap).get(e),n;if(t)return t;if(t=I({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of _(e))if(!A.call(t,r))I(t,r,{get:T.bind(e,r),enumerable:!(n=P(e,r))||n.enumerable})}return j.set(e,t),t},j;var M=(e)=>e;function W(e,t){this[e]=M.bind(null,t)}var q=(e,t)=>{for(var n in t)I(e,n,{get:t[n],enumerable:!0,configurable:!0,set:W.bind(t,n)})};var V={};q(V,{compileWorkflowJson:()=>E});class O{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,i=e.name||t,o=e.nodes||[],s=e.connections||{},c=n||`${t}.json`,a=new Map;o.forEach((d)=>a.set(d.name,[])),Object.entries(s).forEach(([d,m])=>{if(m.main&&Array.isArray(m.main))m.main.forEach((l,f)=>{if(Array.isArray(l))l.forEach((u)=>{if(u&&u.node&&a.has(u.node))a.get(u.node)?.push({sourceNode:d,outputIndex:f})})})}),o.forEach((d)=>{d.incomingConnections=a.get(d.name)||[]});let y=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),p=new Set(y.map((d)=>d.name));o.forEach((d)=>{let m=new Set,l=new Set,f=[...(a.get(d.name)||[]).map((u)=>u.sourceNode)];while(f.length>0){let u=f.shift();if(l.has(u))continue;if(l.add(u),p.has(u))m.add(u);(a.get(u)||[]).forEach((w)=>f.push(w.sourceNode))}d.ancestorTriggers=Array.from(m)});let g=this.topologicalSort(o,s);return{id:r,name:i,fileName:t,relativePath:c.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:g,triggers:y}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((p)=>{n.set(p.name,p),r.set(p.name,[])}),Object.entries(t).forEach(([p,g])=>{if(g.main&&Array.isArray(g.main))g.main.forEach((d)=>{if(Array.isArray(d))d.forEach((m)=>{if(m&&m.node&&n.has(m.node))r.get(p)?.push(m.node)})})});let i=new Set,o=new Set,s=[],c=(p,g=[])=>{if(i.has(p))return;if(o.has(p)){let l=[...g,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${l}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(p),g.push(p);let d=r.get(p)||[];for(let l of d)c(l,g);g.pop(),o.delete(p),i.add(p);let m=n.get(p);if(m)s.push(m)},a=new Map;e.forEach((p)=>a.set(p.name,0));for(let p of r.values())for(let g of p)a.set(g,(a.get(g)||0)+1);let y=e.map((p)=>p.name).filter((p)=>a.get(p)===0);for(let p of y)c(p);for(let p of e)if(!i.has(p.name))c(p.name);return s.reverse()}}class N{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let c=s.substring(2,s.length-2).trim();if(!c.includes("{{")&&!c.includes("}}"))return`(${this.transformTokens(c)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,c)=>{let a=this.transformTokens(c.trim()),y=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${a}) ?? ''}`),y}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((s,c)=>{o=o.replace(`__EXPR_PLACEHOLDER_${c}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}class k{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=N.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=N.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${i} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${o};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${r}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",i=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=N.parse(o.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let c of s)if(c.name){let a=N.parse(c.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(c.name)}, ${a});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let s of t.conditions.conditions){let c=N.parse(s.leftValue),a=N.parse(s.rightValue);switch(s.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${c} == ${a})`);break;case"notEquals":case"notEqual":r.push(`(${c} != ${a})`);break;case"contains":r.push(`String(${c} ?? '').includes(String(${a} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${c}) > Number(${a}))`);break;case"lt":case"smaller":r.push(`(Number(${c}) < Number(${a}))`);break;case"isEmpty":case"empty":r.push(`(${c} === undefined || ${c} === null || ${c} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${c} !== undefined && ${c} !== null && ${c} !== '')`);break;default:r.push(`Boolean(${c})`)}}let i=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(i):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],i=[];r.forEach((s,c)=>{let a=s.conditions?.conditions?.[0];if(a){let y=N.parse(a.leftValue),p=N.parse(a.rightValue);i.push(`if (${y} == ${p}) { matchedIndex = ${c}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${i.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),i=N.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${N.parse(a.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)s=`JSON.stringify(${N.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${N.parse(a.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=N.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=N.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),i=t.unit||"seconds",s=r*(i==="minutes"?60000:i==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${i})
    await new Promise((resolve) => setTimeout(resolve, ${s}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class b{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",i=`import { WorkflowContext } from '${r}/workflow-context';
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
`,c=new Map;e.orderedNodes.forEach((m,l)=>{let f=`node_${l}_${this.sanitizeIdentifier(m.name)}`;c.set(m.name,f)});let a=e.orderedNodes.filter((m)=>m.type.includes("webhook")||m.type.includes("trigger")||m.type.includes("cron")),y=e.orderedNodes.filter((m)=>!a.includes(m));a.forEach((m)=>{let l=e.orderedNodes.indexOf(m),f=c.get(m.name);o+=`  let ${f}: any = inputItems;
`;let u=(m.parameters?.path||m.name).toLowerCase().trim().replace(/^\/+/,"");s+=`  const triggerNode_${l} = item?.json?.__triggerNode;
`,s+=`  const triggerPath_${l} = item?.json?.__triggerPath;
`;let v=`!triggerNode_${l} || triggerNode_${l} === ${JSON.stringify(m.name)} || triggerPath_${l} === ${JSON.stringify(u)}`,w=`${f} = inputItems;
ctx.setNodeOutput(${JSON.stringify(m.name)}, inputItems);`;s+=this.generateNodeBlock(e.id,m,v,w)}),y.forEach((m)=>{let l=c.get(m.name);o+=`  let ${l}: any = inputItems;
`;let f="inputItems",u="true";if(m.incomingConnections&&m.incomingConnections.length>0){if(m.incomingConnections.length===1){let h=m.incomingConnections[0];f=`(ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex}) ? ctx.getNode(${JSON.stringify(h.sourceNode)})?.all() : null)`}else f=`(${m.incomingConnections.map((S)=>`(ctx.isBranchActive(${JSON.stringify(S.sourceNode)}, ${S.outputIndex}) ? ctx.getNode(${JSON.stringify(S.sourceNode)})?.all() : null)`).join(" || ")})`;u=`(${m.incomingConnections.map((h)=>`ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex})`).join(" || ")})`}let v=k.mapNode({node:m,workflowName:e.name,variableName:l,previousVariableName:f}),w=m.ancestorTriggers??[];if(w.length>0)u=`(${w.map((h)=>`ctx.hasExecutedNode(${JSON.stringify(h)})`).join(" || ")}) && ${u}`;s+=this.generateNodeBlock(e.id,m,u,v,w.length>0?"      ":"    ")});let p=e.orderedNodes[e.orderedNodes.length-1]?.name,g=p?`(ctx.getNode(${JSON.stringify(p)})?.all() || [{ json: item.json }])`:"inputItems",d=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${i}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${o}
${s}
  const finalItems = ${g};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(d)}, ${t});
`}static generateNodeBlock(e,t,n,r,i="    "){let o=[e,t.id||t.name,t.name].map((c)=>JSON.stringify(c)).join(", "),s=i+r.trim().replace(/\n/g,`
${i}`);return`  if (${n}) {
    const finishTrace = ctx.startNodeTrace(${o});
    try {
${s}
    } catch (error) {
      finishTrace?.('error');
      throw error;
    }
    finishTrace?.('completed');
  } else {
    ctx.skipNodeTrace(${o});
  }
`}}class C{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let s=`workflow_${b.sanitizeIdentifier(o.fileName)}`,c=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${c}';
`,n.push(s)});let r="",i=new Set;return e.forEach((o)=>{let s=`workflow_${b.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((a)=>a.type.includes("webhook")).forEach((a)=>{let y=(a.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),p=(a.parameters?.httpMethod||"POST").toUpperCase(),g=p==="ALL"?["GET","POST","PUT","DELETE"]:[p],d=Array.from(new Set([`/${y}`,`/webhook/${y}`]));g.forEach((m)=>{d.forEach((l)=>{let f=`${m}:${l}`;if(i.has(f))return;i.add(f),r+=`
  fastify.route({
    method: '${m}',
    url: '${l}',
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
          __triggerPath: ${JSON.stringify(y)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function E(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=O.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=b.generateWorkflowCode(r),o=C.generateRoutesCode([r]),s=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:s,workflowCode:i,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:E};})();
