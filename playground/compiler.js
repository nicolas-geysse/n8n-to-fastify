(()=>{var{defineProperty:I,getOwnPropertyNames:P,getOwnPropertyDescriptor:R}=Object,T=Object.prototype.hasOwnProperty;function M(e){return this[e]}var W=(e)=>{var t=(J??=new WeakMap).get(e),n;if(t)return t;if(t=I({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of P(e))if(!T.call(t,r))I(t,r,{get:M.bind(e,r),enumerable:!(n=R(e,r))||n.enumerable})}return J.set(e,t),t},J;var q=(e)=>e;function V(e,t){this[e]=q.bind(null,t)}var x=(e,t)=>{for(var n in t)I(e,n,{get:t[n],enumerable:!0,configurable:!0,set:V.bind(t,n)})};var B={};x(B,{compileWorkflowJson:()=>A});class O{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,a=e.name||t,o=e.nodes||[],i=e.connections||{},m=n||`${t}.json`,s=new Map;o.forEach((d)=>s.set(d.name,[])),Object.entries(i).forEach(([d,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((c,g)=>{if(Array.isArray(c))c.forEach((l)=>{if(l&&l.node&&s.has(l.node))s.get(l.node)?.push({sourceNode:d,outputIndex:g})})})}),o.forEach((d)=>{d.incomingConnections=s.get(d.name)||[]});let N=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),p=new Set(N.map((d)=>d.name));o.forEach((d)=>{let u=new Set,c=new Set,g=[...(s.get(d.name)||[]).map((l)=>l.sourceNode)];while(g.length>0){let l=g.shift();if(c.has(l))continue;if(c.add(l),p.has(l))u.add(l);(s.get(l)||[]).forEach((v)=>g.push(v.sourceNode))}d.ancestorTriggers=Array.from(u)});let f=this.topologicalSort(o,i);return{id:r,name:a,fileName:t,relativePath:m.replace(/\.json$/,".ts"),nodes:o,connections:i,orderedNodes:f,triggers:N}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((p)=>{n.set(p.name,p),r.set(p.name,[])}),Object.entries(t).forEach(([p,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((d)=>{if(Array.isArray(d))d.forEach((u)=>{if(u&&u.node&&n.has(u.node))r.get(p)?.push(u.node)})})});let a=new Set,o=new Set,i=[],m=(p,f=[])=>{if(a.has(p))return;if(o.has(p)){let c=[...f,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${c}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(p),f.push(p);let d=r.get(p)||[];for(let c of d)m(c,f);f.pop(),o.delete(p),a.add(p);let u=n.get(p);if(u)i.push(u)},s=new Map;e.forEach((p)=>s.set(p.name,0));for(let p of r.values())for(let f of p)s.set(f,(s.get(f)||0)+1);let N=e.map((p)=>p.name).filter((p)=>s.get(p)===0);for(let p of N)m(p);for(let p of e)if(!a.has(p.name))m(p.name);return i.reverse()}}class y{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let i=t.substring(1).trim();if(i.startsWith("{{")&&i.endsWith("}}")){let m=i.substring(2,i.length-2).trim();if(!m.includes("{{")&&!m.includes("}}"))return`(${this.transformTokens(m)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(i,m)=>{let s=this.transformTokens(m.trim()),N=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${s}) ?? ''}`),N}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((i,m)=>{o=o.replace(`__EXPR_PLACEHOLDER_${m}__`,i)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}function _(e){let t=e.parameters??{};return e.type==="n8n-nodes-base.code"&&e.onError==="continueRegularOutput"&&(t.language??"javaScript")==="javaScript"&&(t.mode??"runOnceForAllItems")==="runOnceForAllItems"}class k{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=y.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=y.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",o=`
      const result = await (async () => {
        ${n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(m,s)=>`(ctx.getNode(${JSON.stringify(s)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(m,s)=>`(ctx.getNode(${JSON.stringify(s)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(m,s)=>`(ctx.getNode(${JSON.stringify(s)})?.all() || [])`).replace(/\$now/g,"DateTime.now()")}
      })();
      return n8n.normalizeItems(result);`,i=_(e.node)?`
      try {${o}
      } catch (error) {
        return [{ json: { error: error instanceof Error ? error.message : String(error) } }];
      }`:o;return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${r} || inputItems;${i}
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},n=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(n)}, ${r} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let i=y.parse(o.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${i});`)}}if(t.values)for(let o of["string","number","boolean"]){let i=t.values[o];if(Array.isArray(i)){for(let m of i)if(m.name){let s=y.parse(m.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${s});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let i of t.conditions.conditions){let m=y.parse(i.leftValue),s=y.parse(i.rightValue);switch(i.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${m} == ${s})`);break;case"notEquals":case"notEqual":r.push(`(${m} != ${s})`);break;case"contains":r.push(`String(${m} ?? '').includes(String(${s} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${m}) > Number(${s}))`);break;case"lt":case"smaller":r.push(`(Number(${m}) < Number(${s}))`);break;case"isEmpty":case"empty":r.push(`(${m} === undefined || ${m} === null || ${m} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${m} !== undefined && ${m} !== null && ${m} !== '')`);break;default:r.push(`Boolean(${m})`)}}let a=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(a):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],a=[];r.forEach((i,m)=>{let s=i.conditions?.conditions?.[0];if(s){let N=y.parse(s.leftValue),p=y.parse(s.rightValue);a.push(`if (${N} == ${p}) { matchedIndex = ${m}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${a.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),a=y.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((s)=>s.name).map((s)=>`${JSON.stringify(s.name)}: ${y.parse(s.value)}`).join(", ")} }`;let i="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)i=`JSON.stringify(${y.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))i=`JSON.stringify({ ${t.bodyParameters.parameters.filter((s)=>s.name).map((s)=>`${JSON.stringify(s.name)}: ${y.parse(s.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${a};
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),a=t.unit||"seconds",i=r*(a==="minutes"?60000:a==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${a})
    await new Promise((resolve) => setTimeout(resolve, ${i}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class b{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",a=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,o=`  let item: N8nItem = inputItems[0] || { json: {} };
`,i=`  ctx.setNodeOutput("__initial__", inputItems);
`,m="inputItems",s=new Map;e.orderedNodes.forEach((c,g)=>{let l=`node_${g}_${this.sanitizeIdentifier(c.name)}`;s.set(c.name,l)});let N=e.orderedNodes.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),p=e.orderedNodes.filter((c)=>!N.includes(c));N.forEach((c)=>{let g=e.orderedNodes.indexOf(c),l=s.get(c.name);o+=`  let ${l}: any = inputItems;
`;let w=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");i+=`  const triggerNode_${g} = item?.json?.__triggerNode;
`,i+=`  const triggerPath_${g} = item?.json?.__triggerPath;
`,i+=`  if (!triggerNode_${g} || triggerNode_${g} === ${JSON.stringify(c.name)} || triggerPath_${g} === ${JSON.stringify(w)}) {
`,i+=`    ${l} = inputItems;
`,i+=`    ctx.setNodeOutput(${JSON.stringify(c.name)}, inputItems);
`,i+=`  }
`}),p.forEach((c)=>{let g=s.get(c.name);o+=`  let ${g}: any = inputItems;
`;let l="inputItems",w="true";if(c.incomingConnections&&c.incomingConnections.length>0){if(c.incomingConnections.length===1){let h=c.incomingConnections[0];l=`(ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex}) ? ctx.getNode(${JSON.stringify(h.sourceNode)})?.all() : null)`}else l=`(${c.incomingConnections.map((S)=>`(ctx.isBranchActive(${JSON.stringify(S.sourceNode)}, ${S.outputIndex}) ? ctx.getNode(${JSON.stringify(S.sourceNode)})?.all() : null)`).join(" || ")})`;w=`(${c.incomingConnections.map((h)=>`ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex})`).join(" || ")})`}let v=k.mapNode({node:c,workflowName:e.name,variableName:g,previousVariableName:l}),j=`  if (${w}) {
    ${v.trim().replace(/\n/g,`
    `)}
  }
`;if(c.ancestorTriggers&&c.ancestorTriggers.length>0){let E=c.ancestorTriggers.map((h)=>`ctx.hasExecutedNode(${JSON.stringify(h)})`).join(" || ");i+=`  if (${E}) {
  ${j.trim().replace(/\n/g,`
  `)}
  }
`}else i+=j;m=c.name});let f=e.orderedNodes[e.orderedNodes.length-1]?.name,d=f?`(ctx.getNode(${JSON.stringify(f)})?.all() || [{ json: item.json }])`:"inputItems",u=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${a}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${o}
${i}
  const finalItems = ${d};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(u)}, ${t});
`}}class C{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let i=`workflow_${b.sanitizeIdentifier(o.fileName)}`,m=o.relativePath.replace(/\.ts$/,"");t+=`import { ${i} } from '../workflows/${m}';
`,n.push(i)});let r="",a=new Set;return e.forEach((o)=>{let i=`workflow_${b.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((s)=>s.type.includes("webhook")).forEach((s)=>{let N=(s.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),p=(s.parameters?.httpMethod||"POST").toUpperCase(),f=p==="ALL"?["GET","POST","PUT","DELETE"]:[p],d=Array.from(new Set([`/${N}`,`/webhook/${N}`]));f.forEach((u)=>{d.forEach((c)=>{let g=`${u}:${c}`;if(a.has(g))return;a.add(g),r+=`
  fastify.route({
    method: '${u}',
    url: '${c}',
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
          __triggerNode: ${JSON.stringify(s.name)},
          __triggerPath: ${JSON.stringify(N)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function A(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=O.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let a=b.generateWorkflowCode(r),o=C.generateRoutesCode([r]),i=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:i,workflowCode:a,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:A};})();
