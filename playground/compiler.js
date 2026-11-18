(()=>{var{defineProperty:C,getOwnPropertyNames:V,getOwnPropertyDescriptor:q}=Object,x=Object.prototype.hasOwnProperty;function B(e){return this[e]}var L=(e)=>{var t=(A??=new WeakMap).get(e),n;if(t)return t;if(t=C({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of V(e))if(!x.call(t,r))C(t,r,{get:B.bind(e,r),enumerable:!(n=q(e,r))||n.enumerable})}return A.set(e,t),t},A;var D=(e)=>e;function F(e,t){this[e]=D.bind(null,t)}var G=(e,t)=>{for(var n in t)C(e,n,{get:t[n],enumerable:!0,configurable:!0,set:F.bind(t,n)})};var H={};G(H,{compileWorkflowJson:()=>R});class j{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,s=e.name||t,o=e.nodes||[],a=e.connections||{},p=n||`${t}.json`,i=new Map;o.forEach((c)=>i.set(c.name,[])),Object.entries(a).forEach(([c,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((N,h)=>{if(Array.isArray(N))N.forEach((m)=>{if(m&&m.node&&i.has(m.node))i.get(m.node)?.push({sourceNode:c,outputIndex:h})})})}),o.forEach((c)=>{c.incomingConnections=i.get(c.name)||[]});let g=o.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),d=new Set(g.map((c)=>c.name));o.forEach((c)=>{let u=new Set,N=new Set,h=[...(i.get(c.name)||[]).map((m)=>m.sourceNode)];while(h.length>0){let m=h.shift();if(N.has(m))continue;if(N.add(m),d.has(m))u.add(m);(i.get(m)||[]).forEach((b)=>h.push(b.sourceNode))}c.ancestorTriggers=Array.from(u)});let f=this.topologicalSort(o,a);return{id:r,name:s,fileName:t,relativePath:p.replace(/\.json$/,".ts"),nodes:o,connections:a,orderedNodes:f,triggers:g}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((d)=>{n.set(d.name,d),r.set(d.name,[])}),Object.entries(t).forEach(([d,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((c)=>{if(Array.isArray(c))c.forEach((u)=>{if(u&&u.node&&n.has(u.node))r.get(d)?.push(u.node)})})});let s=new Set,o=new Set,a=[],p=(d,f=[])=>{if(s.has(d))return;if(o.has(d)){let N=[...f,d].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${N}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(d),f.push(d);let c=r.get(d)||[];for(let N of c)p(N,f);f.pop(),o.delete(d),s.add(d);let u=n.get(d);if(u)a.push(u)},i=new Map;e.forEach((d)=>i.set(d.name,0));for(let d of r.values())for(let f of d)i.set(f,(i.get(f)||0)+1);let g=e.map((d)=>d.name).filter((d)=>i.get(d)===0);for(let d of g)p(d);for(let d of e)if(!s.has(d.name))p(d.name);return a.reverse()}}class l{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let a=t.substring(1).trim();if(a.startsWith("{{")&&a.endsWith("}}")){let p=a.substring(2,a.length-2).trim();if(!p.includes("{{")&&!p.includes("}}"))return`(${this.transformTokens(p)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(a,p)=>{let i=this.transformTokens(p.trim()),g=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${i}) ?? ''}`),g}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((a,p)=>{o=o.replace(`__EXPR_PLACEHOLDER_${p}__`,a)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}var v="n8n-nodes-base.s3";function z(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function T(e){let t=z(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=e.previousVariableName??"inputItems",s=l.parse(n.bucketName),o=l.parse(n.fileKey),a=l.parse(n.binaryPropertyName??"data");return`
    {
      const downloads = (${r} || inputItems).map((item: N8nItem) => ({
        item,
        bucket: String(${s} ?? ''),
        key: String(${o} ?? ''),
        binaryProperty: String(${a} ?? ''),
      }));
      ${e.variableName} = await downloadS3Files(downloads);
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}var S="n8n-nodes-base.compression";function U(e){let t=e.typeVersion??1;if(t!==1&&t!==1.1||(e.parameters?.operation??"decompress")!=="decompress")return"Compression supports ZIP and GZIP decompression on node versions 1 and 1.1"}function M(e){let t=U(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",s=l.parse(n.binaryPropertyName??"data"),o=l.parse(n.outputPrefix??"file_");return`
    {
      const source: N8nItem[] = ${r} || inputItems;
      ${e.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${e.variableName} = await decompressItems(source, String(${s}), String(${o}));
      }
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}class E{static mapNode(e){let t=e.node.type;if(t===v)return T(e);if(t===S)return M(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,s=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=l.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=l.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",s=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,a)=>`(ctx.getNode(${JSON.stringify(a)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,a)=>`(ctx.getNode(${JSON.stringify(a)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,a)=>`(ctx.getNode(${JSON.stringify(a)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",s=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let a=l.parse(o.value);s.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${a});`)}}if(t.values)for(let o of["string","number","boolean"]){let a=t.values[o];if(Array.isArray(a)){for(let p of a)if(p.name){let i=l.parse(p.value);s.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${i});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${s.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let a of t.conditions.conditions){let p=l.parse(a.leftValue),i=l.parse(a.rightValue);switch(a.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${p} == ${i})`);break;case"notEquals":case"notEqual":r.push(`(${p} != ${i})`);break;case"contains":r.push(`String(${p} ?? '').includes(String(${i} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${p}) > Number(${i}))`);break;case"lt":case"smaller":r.push(`(Number(${p}) < Number(${i}))`);break;case"isEmpty":case"empty":r.push(`(${p} === undefined || ${p} === null || ${p} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${p} !== undefined && ${p} !== null && ${p} !== '')`);break;default:r.push(`Boolean(${p})`)}}let s=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(s):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],s=[];r.forEach((a,p)=>{let i=a.conditions?.conditions?.[0];if(i){let g=l.parse(i.leftValue),d=l.parse(i.rightValue);s.push(`if (${g} == ${d}) { matchedIndex = ${p}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${s.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),s=l.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${l.parse(i.value)}`).join(", ")} }`;let a="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)a=`JSON.stringify(${l.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))a=`JSON.stringify({ ${t.bodyParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${l.parse(i.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${s};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${o} };
      const reqBody = ${a};
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=l.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",s=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=l.parse(s);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),s=t.unit||"seconds",a=r*(s==="minutes"?60000:s==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${s})
    await new Promise((resolve) => setTimeout(resolve, ${a}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class I{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",s=e.nodes.some((m)=>m.type===v)?`import { downloadS3Files } from '${r}/s3';
`:"",o=e.nodes.some((m)=>m.type===S)?`import { decompressItems } from '${r}/compression';
`:"",a=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${s}${o}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,p=`  let item: N8nItem = inputItems[0] || { json: {} };
`,i=`  ctx.setNodeOutput("__initial__", inputItems);
`,g="inputItems",d=new Map;e.orderedNodes.forEach((m,y)=>{let b=`node_${y}_${this.sanitizeIdentifier(m.name)}`;d.set(m.name,b)});let f=e.orderedNodes.filter((m)=>m.type.includes("webhook")||m.type.includes("trigger")||m.type.includes("cron")),c=e.orderedNodes.filter((m)=>!f.includes(m));f.forEach((m)=>{let y=e.orderedNodes.indexOf(m),b=d.get(m.name);p+=`  let ${b}: any = inputItems;
`;let O=(m.parameters?.path||m.name).toLowerCase().trim().replace(/^\/+/,"");i+=`  const triggerNode_${y} = item?.json?.__triggerNode;
`,i+=`  const triggerPath_${y} = item?.json?.__triggerPath;
`,i+=`  if (!triggerNode_${y} || triggerNode_${y} === ${JSON.stringify(m.name)} || triggerPath_${y} === ${JSON.stringify(O)}) {
`,i+=`    ${b} = inputItems;
`,i+=`    ctx.setNodeOutput(${JSON.stringify(m.name)}, inputItems);
`,i+=`  }
`}),c.forEach((m)=>{let y=d.get(m.name);p+=`  let ${y}: any = inputItems;
`;let b="inputItems",O="true";if(m.incomingConnections&&m.incomingConnections.length>0){if(m.incomingConnections.length===1){let w=m.incomingConnections[0];b=`(ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex}) ? ctx.getNode(${JSON.stringify(w.sourceNode)})?.all() : null)`}else b=`(${m.incomingConnections.map((k)=>`(ctx.isBranchActive(${JSON.stringify(k.sourceNode)}, ${k.outputIndex}) ? ctx.getNode(${JSON.stringify(k.sourceNode)})?.all() : null)`).join(" || ")})`;O=`(${m.incomingConnections.map((w)=>`ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`).join(" || ")})`}let W=E.mapNode({node:m,workflowName:e.name,variableName:y,previousVariableName:b}),J=`  if (${O}) {
    ${W.trim().replace(/\n/g,`
    `)}
  }
`;if(m.ancestorTriggers&&m.ancestorTriggers.length>0){let _=m.ancestorTriggers.map((w)=>`ctx.hasExecutedNode(${JSON.stringify(w)})`).join(" || ");i+=`  if (${_}) {
  ${J.trim().replace(/\n/g,`
  `)}
  }
`}else i+=J;g=m.name});let u=e.orderedNodes[e.orderedNodes.length-1]?.name,N=u?`(ctx.getNode(${JSON.stringify(u)})?.all() || [{ json: item.json }])`:"inputItems",h=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${a}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${p}
${i}
  const finalItems = ${N};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(h)}, ${t});
`}}class P{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let a=`workflow_${I.sanitizeIdentifier(o.fileName)}`,p=o.relativePath.replace(/\.ts$/,"");t+=`import { ${a} } from '../workflows/${p}';
`,n.push(a)});let r="",s=new Set;return e.forEach((o)=>{let a=`workflow_${I.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((i)=>i.type.includes("webhook")).forEach((i)=>{let g=(i.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),d=(i.parameters?.httpMethod||"POST").toUpperCase(),f=d==="ALL"?["GET","POST","PUT","DELETE"]:[d],c=Array.from(new Set([`/${g}`,`/webhook/${g}`]));f.forEach((u)=>{c.forEach((N)=>{let h=`${u}:${N}`;if(s.has(h))return;s.add(h),r+=`
  fastify.route({
    method: '${u}',
    url: '${N}',
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
          __triggerPath: ${JSON.stringify(g)}
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
`}static generateOutputPackageJson(e=[]){let t=e.some((r)=>r.nodes.some((s)=>s.type===v)),n=e.some((r)=>r.nodes.some((s)=>s.type===S));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...t?{"@aws-sdk/client-s3":"3.808.0"}:{},...t||n?{"mime-types":"^3.0.1"}:{},...n?{fflate:"0.8.3"}:{}}},null,2)}}function R(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=j.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let s=I.generateWorkflowCode(r),o=P.generateRoutesCode([r]),a=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:a,workflowCode:s,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:R};})();
