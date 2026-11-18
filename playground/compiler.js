(()=>{var{defineProperty:O,getOwnPropertyNames:R,getOwnPropertyDescriptor:M}=Object,W=Object.prototype.hasOwnProperty;function V(e){return this[e]}var q=(e)=>{var t=(J??=new WeakMap).get(e),n;if(t)return t;if(t=O({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of R(e))if(!W.call(t,r))O(t,r,{get:V.bind(e,r),enumerable:!(n=M(e,r))||n.enumerable})}return J.set(e,t),t},J;var x=(e)=>e;function B(e,t){this[e]=x.bind(null,t)}var L=(e,t)=>{for(var n in t)O(e,n,{get:t[n],enumerable:!0,configurable:!0,set:B.bind(t,n)})};var F={};L(F,{compileWorkflowJson:()=>A});class k{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,i=e.name||t,o=e.nodes||[],s=e.connections||{},a=n||`${t}.json`,m=new Map;o.forEach((c)=>m.set(c.name,[])),Object.entries(s).forEach(([c,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((y,p)=>{if(Array.isArray(y))y.forEach((l)=>{if(l&&l.node&&m.has(l.node))m.get(l.node)?.push({sourceNode:c,outputIndex:p})})})}),o.forEach((c)=>{c.incomingConnections=m.get(c.name)||[]});let g=o.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),d=new Set(g.map((c)=>c.name));o.forEach((c)=>{let f=new Set,y=new Set,p=[...(m.get(c.name)||[]).map((l)=>l.sourceNode)];while(p.length>0){let l=p.shift();if(y.has(l))continue;if(y.add(l),d.has(l))f.add(l);(m.get(l)||[]).forEach((b)=>p.push(b.sourceNode))}c.ancestorTriggers=Array.from(f)});let N=this.topologicalSort(o,s);return{id:r,name:i,fileName:t,relativePath:a.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:N,triggers:g}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((d)=>{n.set(d.name,d),r.set(d.name,[])}),Object.entries(t).forEach(([d,N])=>{if(N.main&&Array.isArray(N.main))N.main.forEach((c)=>{if(Array.isArray(c))c.forEach((f)=>{if(f&&f.node&&n.has(f.node))r.get(d)?.push(f.node)})})});let i=new Set,o=new Set,s=[],a=(d,N=[])=>{if(i.has(d))return;if(o.has(d)){let y=[...N,d].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(d),N.push(d);let c=r.get(d)||[];for(let y of c)a(y,N);N.pop(),o.delete(d),i.add(d);let f=n.get(d);if(f)s.push(f)},m=new Map;e.forEach((d)=>m.set(d.name,0));for(let d of r.values())for(let N of d)m.set(N,(m.get(N)||0)+1);let g=e.map((d)=>d.name).filter((d)=>m.get(d)===0);for(let d of g)a(d);for(let d of e)if(!i.has(d.name))a(d.name);return s.reverse()}}class u{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let a=s.substring(2,s.length-2).trim();if(!a.includes("{{")&&!a.includes("}}"))return`(${this.transformTokens(a)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,a)=>{let m=this.transformTokens(a.trim()),g=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${m}) ?? ''}`),g}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((s,a)=>{o=o.replace(`__EXPR_PLACEHOLDER_${a}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}var v="n8n-nodes-base.s3";function D(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function _(e){let t=D(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=e.previousVariableName??"inputItems",i=u.parse(n.bucketName),o=u.parse(n.fileKey),s=u.parse(n.binaryPropertyName??"data");return`
    {
      const downloads = (${r} || inputItems).map((item: N8nItem) => ({
        item,
        bucket: String(${i} ?? ''),
        key: String(${o} ?? ''),
        binaryProperty: String(${s} ?? ''),
      }));
      ${e.variableName} = await downloadS3Files(downloads);
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}class C{static mapNode(e){let t=e.node.type;if(t===v)return _(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=u.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=u.parse(t.responseBody);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=u.parse(o.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let a of s)if(a.name){let m=u.parse(a.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(a.name)}, ${m});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let s of t.conditions.conditions){let a=u.parse(s.leftValue),m=u.parse(s.rightValue);switch(s.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${a} == ${m})`);break;case"notEquals":case"notEqual":r.push(`(${a} != ${m})`);break;case"contains":r.push(`String(${a} ?? '').includes(String(${m} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${a}) > Number(${m}))`);break;case"lt":case"smaller":r.push(`(Number(${a}) < Number(${m}))`);break;case"isEmpty":case"empty":r.push(`(${a} === undefined || ${a} === null || ${a} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${a} !== undefined && ${a} !== null && ${a} !== '')`);break;default:r.push(`Boolean(${a})`)}}let i=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(i):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],i=[];r.forEach((s,a)=>{let m=s.conditions?.conditions?.[0];if(m){let g=u.parse(m.leftValue),d=u.parse(m.rightValue);i.push(`if (${g} == ${d}) { matchedIndex = ${a}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${i.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),i=u.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${u.parse(m.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)s=`JSON.stringify(${u.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${u.parse(m.value)}`).join(", ")} })`}return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=u.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class S{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",i=e.nodes.some((p)=>p.type===v)?`import { downloadS3Files } from '${r}/s3';
`:"",o=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${i}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,s=`  let item: N8nItem = inputItems[0] || { json: {} };
`,a=`  ctx.setNodeOutput("__initial__", inputItems);
`,m="inputItems",g=new Map;e.orderedNodes.forEach((p,l)=>{let h=`node_${l}_${this.sanitizeIdentifier(p.name)}`;g.set(p.name,h)});let d=e.orderedNodes.filter((p)=>p.type.includes("webhook")||p.type.includes("trigger")||p.type.includes("cron")),N=e.orderedNodes.filter((p)=>!d.includes(p));d.forEach((p)=>{let l=e.orderedNodes.indexOf(p),h=g.get(p.name);s+=`  let ${h}: any = inputItems;
`;let b=(p.parameters?.path||p.name).toLowerCase().trim().replace(/^\/+/,"");a+=`  const triggerNode_${l} = item?.json?.__triggerNode;
`,a+=`  const triggerPath_${l} = item?.json?.__triggerPath;
`,a+=`  if (!triggerNode_${l} || triggerNode_${l} === ${JSON.stringify(p.name)} || triggerPath_${l} === ${JSON.stringify(b)}) {
`,a+=`    ${h} = inputItems;
`,a+=`    ctx.setNodeOutput(${JSON.stringify(p.name)}, inputItems);
`,a+=`  }
`}),N.forEach((p)=>{let l=g.get(p.name);s+=`  let ${l}: any = inputItems;
`;let h="inputItems",b="true";if(p.incomingConnections&&p.incomingConnections.length>0){if(p.incomingConnections.length===1){let w=p.incomingConnections[0];h=`(ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex}) ? ctx.getNode(${JSON.stringify(w.sourceNode)})?.all() : null)`}else h=`(${p.incomingConnections.map((I)=>`(ctx.isBranchActive(${JSON.stringify(I.sourceNode)}, ${I.outputIndex}) ? ctx.getNode(${JSON.stringify(I.sourceNode)})?.all() : null)`).join(" || ")})`;b=`(${p.incomingConnections.map((w)=>`ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`).join(" || ")})`}let T=C.mapNode({node:p,workflowName:e.name,variableName:l,previousVariableName:h}),E=`  if (${b}) {
    ${T.trim().replace(/\n/g,`
    `)}
  }
`;if(p.ancestorTriggers&&p.ancestorTriggers.length>0){let P=p.ancestorTriggers.map((w)=>`ctx.hasExecutedNode(${JSON.stringify(w)})`).join(" || ");a+=`  if (${P}) {
  ${E.trim().replace(/\n/g,`
  `)}
  }
`}else a+=E;m=p.name});let c=e.orderedNodes[e.orderedNodes.length-1]?.name,f=c?`(ctx.getNode(${JSON.stringify(c)})?.all() || [{ json: item.json }])`:"inputItems",y=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${o}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${s}
${a}
  const finalItems = ${f};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(y)}, ${t});
`}}class j{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`,a=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${a}';
`,n.push(s)});let r="",i=new Set;return e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((m)=>m.type.includes("webhook")).forEach((m)=>{let g=(m.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),d=(m.parameters?.httpMethod||"POST").toUpperCase(),N=d==="ALL"?["GET","POST","PUT","DELETE"]:[d],c=Array.from(new Set([`/${g}`,`/webhook/${g}`]));N.forEach((f)=>{c.forEach((y)=>{let p=`${f}:${y}`;if(i.has(p))return;i.add(p),r+=`
  fastify.route({
    method: '${f}',
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
          __triggerNode: ${JSON.stringify(m.name)},
          __triggerPath: ${JSON.stringify(g)}
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
`}static generateOutputPackageJson(e=[]){let t=e.some((n)=>n.nodes.some((r)=>r.type===v));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...t?{"@aws-sdk/client-s3":"3.808.0","mime-types":"^3.0.1"}:{}}},null,2)}}function A(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=k.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=S.generateWorkflowCode(r),o=j.generateRoutesCode([r]),s=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:s,workflowCode:i,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:A};})();
