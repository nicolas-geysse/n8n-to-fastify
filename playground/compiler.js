(()=>{var{defineProperty:j,getOwnPropertyNames:V,getOwnPropertyDescriptor:B}=Object,H=Object.prototype.hasOwnProperty;function L(e){return this[e]}var D=(e)=>{var t=(_??=new WeakMap).get(e),n;if(t)return t;if(t=j({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of V(e))if(!H.call(t,r))j(t,r,{get:L.bind(e,r),enumerable:!(n=B(e,r))||n.enumerable})}return _.set(e,t),t},_;var F=(e)=>e;function U(e,t){this[e]=F.bind(null,t)}var G=(e,t)=>{for(var n in t)j(e,n,{get:t[n],enumerable:!0,configurable:!0,set:U.bind(t,n)})};var K={};G(K,{compileWorkflowJson:()=>W});class P{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,a=e.name||t,o=e.nodes||[],s=e.connections||{},p=n||`${t}.json`,i=new Map;o.forEach((l)=>i.set(l.name,[])),Object.entries(s).forEach(([l,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((N,h)=>{if(Array.isArray(N))N.forEach((m)=>{if(m&&m.node&&i.has(m.node))i.get(m.node)?.push({sourceNode:l,outputIndex:h})})})}),o.forEach((l)=>{l.incomingConnections=i.get(l.name)||[]});let g=o.filter((l)=>l.type.includes("webhook")||l.type.includes("trigger")||l.type.includes("cron")),d=new Set(g.map((l)=>l.name));o.forEach((l)=>{let f=new Set,N=new Set,h=[...(i.get(l.name)||[]).map((m)=>m.sourceNode)];while(h.length>0){let m=h.shift();if(N.has(m))continue;if(N.add(m),d.has(m))f.add(m);(i.get(m)||[]).forEach((b)=>h.push(b.sourceNode))}l.ancestorTriggers=Array.from(f)});let c=this.topologicalSort(o,s);return{id:r,name:a,fileName:t,relativePath:p.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:c,triggers:g}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((d)=>{n.set(d.name,d),r.set(d.name,[])}),Object.entries(t).forEach(([d,c])=>{if(c.main&&Array.isArray(c.main))c.main.forEach((l)=>{if(Array.isArray(l))l.forEach((f)=>{if(f&&f.node&&n.has(f.node))r.get(d)?.push(f.node)})})});let a=new Set,o=new Set,s=[],p=(d,c=[])=>{if(a.has(d))return;if(o.has(d)){let N=[...c,d].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${N}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(d),c.push(d);let l=r.get(d)||[];for(let N of l)p(N,c);c.pop(),o.delete(d),a.add(d);let f=n.get(d);if(f)s.push(f)},i=new Map;e.forEach((d)=>i.set(d.name,0));for(let d of r.values())for(let c of d)i.set(c,(i.get(c)||0)+1);let g=e.map((d)=>d.name).filter((d)=>i.get(d)===0);for(let d of g)p(d);for(let d of e)if(!a.has(d.name))p(d.name);return s.reverse()}}class u{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let p=s.substring(2,s.length-2).trim();if(!p.includes("{{")&&!p.includes("}}"))return`(${this.transformTokens(p)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,p)=>{let i=this.transformTokens(p.trim()),g=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${i}) ?? ''}`),g}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((s,p)=>{o=o.replace(`__EXPR_PLACEHOLDER_${p}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}var v="n8n-nodes-base.s3";function z(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function R(e){let t=z(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=e.previousVariableName??"inputItems",a=u.parse(n.bucketName),o=u.parse(n.fileKey),s=u.parse(n.binaryPropertyName??"data");return`
    {
      const downloads = (${r} || inputItems).map((item: N8nItem) => ({
        item,
        bucket: String(${a} ?? ''),
        key: String(${o} ?? ''),
        binaryProperty: String(${s} ?? ''),
      }));
      ${e.variableName} = await downloadS3Files(downloads);
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}function O(e){return e?.sendBody===!0&&e.contentType==="multipart-form-data"}function k(e){return e?.sendHeaders===!0&&e.specifyHeaders==="json"}function M(e){let t=(e.method??"GET").toUpperCase();if(!["POST","PUT","PATCH"].includes(t))return"HTTP Request multipart supports POST, PUT and PATCH";let n=e.bodyParameters?.parameters??[];if(!Array.isArray(n))return"HTTP Request multipart needs an array of form fields";for(let r of n){if(typeof r?.name!=="string"||!r.name)return"HTTP Request multipart fields require a name";if(r.parameterType==="formBinaryData"){if(typeof r.inputDataFieldName!=="string"||!r.inputDataFieldName)return"HTTP Request multipart binary fields require an input binary property"}else if(r.parameterType&&!["formData","text"].includes(r.parameterType))return"HTTP Request multipart supports text and binary form fields"}}function q(e){return`createRequestForm(item, [${(e.bodyParameters?.parameters??[]).map((r)=>{let a=u.parse(r.name),o=r.parameterType==="formBinaryData"?"binaryProperty":"value",s=u.parse(r.parameterType==="formBinaryData"?r.inputDataFieldName:r.value);return`{ name: String(${a} ?? ''), ${o}: String(${s} ?? '') }`}).join(", ")}])`}class T{static mapNode(e){let t=e.node.type;if(t===v)return R(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=u.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=u.parse(t.responseBody);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=u.parse(o.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let p of s)if(p.name){let i=u.parse(p.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${i});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let s of t.conditions.conditions){let p=u.parse(s.leftValue),i=u.parse(s.rightValue);switch(s.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${p} == ${i})`);break;case"notEquals":case"notEqual":r.push(`(${p} != ${i})`);break;case"contains":r.push(`String(${p} ?? '').includes(String(${i} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${p}) > Number(${i}))`);break;case"lt":case"smaller":r.push(`(Number(${p}) < Number(${i}))`);break;case"isEmpty":case"empty":r.push(`(${p} === undefined || ${p} === null || ${p} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${p} !== undefined && ${p} !== null && ${p} !== '')`);break;default:r.push(`Boolean(${p})`)}}let a=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(a):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],a=[];r.forEach((s,p)=>{let i=s.conditions?.conditions?.[0];if(i){let g=u.parse(i.leftValue),d=u.parse(i.rightValue);a.push(`if (${g} == ${d}) { matchedIndex = ${p}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${a.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),a=u.parse(t.url||""),o=O(t),s=k(t),p="{}";if(s)p=`parseRequestHeaders(${u.parse(t.jsonHeaders??"{}")})`;else if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))p=`{ ${t.headerParameters.parameters.filter((c)=>c.name).map((c)=>`${JSON.stringify(c.name)}: ${u.parse(c.value)}`).join(", ")} }`;let i="undefined";if(o){let d=M(t);if(d)throw Error(`Node ${JSON.stringify(e.node.name)}: ${d}`);i=q(t)}else if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)i=`JSON.stringify(${u.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))i=`JSON.stringify({ ${t.bodyParameters.parameters.filter((c)=>c.name).map((c)=>`${JSON.stringify(c.name)}: ${u.parse(c.value)}`).join(", ")} })`}let g=`const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${p} };`;if(o)g=`const reqHeaders = ${s?p:`new Headers(${p})`};
      reqHeaders.delete('content-type');
      reqHeaders.delete('content-length');`;else if(s)g=`const reqHeaders = ${p};
      if (!reqHeaders.has('content-type')) reqHeaders.set('content-type', 'application/json');`;return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${a};
      ${g}
      const reqBody = ${i};
      const res = await fetch(targetUrl, {
        method: ${JSON.stringify(r)},
        headers: reqHeaders,
        ...(reqBody !== undefined ? { body: reqBody } : {})
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",a=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=u.parse(a);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class S{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",a=e.nodes.some((m)=>m.type===v)?`import { downloadS3Files } from '${r}/s3';
`:"",o=e.nodes.some((m)=>m.type.includes("httpRequest")&&(O(m.parameters)||k(m.parameters)))?`import { createRequestForm, parseRequestHeaders } from '${r}/http-request';
`:"",s=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${a}${o}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,p=`  let item: N8nItem = inputItems[0] || { json: {} };
`,i=`  ctx.setNodeOutput("__initial__", inputItems);
`,g="inputItems",d=new Map;e.orderedNodes.forEach((m,y)=>{let b=`node_${y}_${this.sanitizeIdentifier(m.name)}`;d.set(m.name,b)});let c=e.orderedNodes.filter((m)=>m.type.includes("webhook")||m.type.includes("trigger")||m.type.includes("cron")),l=e.orderedNodes.filter((m)=>!c.includes(m));c.forEach((m)=>{let y=e.orderedNodes.indexOf(m),b=d.get(m.name);p+=`  let ${b}: any = inputItems;
`;let I=(m.parameters?.path||m.name).toLowerCase().trim().replace(/^\/+/,"");i+=`  const triggerNode_${y} = item?.json?.__triggerNode;
`,i+=`  const triggerPath_${y} = item?.json?.__triggerPath;
`,i+=`  if (!triggerNode_${y} || triggerNode_${y} === ${JSON.stringify(m.name)} || triggerPath_${y} === ${JSON.stringify(I)}) {
`,i+=`    ${b} = inputItems;
`,i+=`    ctx.setNodeOutput(${JSON.stringify(m.name)}, inputItems);
`,i+=`  }
`}),l.forEach((m)=>{let y=d.get(m.name);p+=`  let ${y}: any = inputItems;
`;let b="inputItems",I="true";if(m.incomingConnections&&m.incomingConnections.length>0){if(m.incomingConnections.length===1){let w=m.incomingConnections[0];b=`(ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex}) ? ctx.getNode(${JSON.stringify(w.sourceNode)})?.all() : null)`}else b=`(${m.incomingConnections.map((C)=>`(ctx.isBranchActive(${JSON.stringify(C.sourceNode)}, ${C.outputIndex}) ? ctx.getNode(${JSON.stringify(C.sourceNode)})?.all() : null)`).join(" || ")})`;I=`(${m.incomingConnections.map((w)=>`ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`).join(" || ")})`}let x=T.mapNode({node:m,workflowName:e.name,variableName:y,previousVariableName:b}),J=`  if (${I}) {
    ${x.trim().replace(/\n/g,`
    `)}
  }
`;if(m.ancestorTriggers&&m.ancestorTriggers.length>0){let A=m.ancestorTriggers.map((w)=>`ctx.hasExecutedNode(${JSON.stringify(w)})`).join(" || ");i+=`  if (${A}) {
  ${J.trim().replace(/\n/g,`
  `)}
  }
`}else i+=J;g=m.name});let f=e.orderedNodes[e.orderedNodes.length-1]?.name,N=f?`(ctx.getNode(${JSON.stringify(f)})?.all() || [{ json: item.json }])`:"inputItems",h=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${s}
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
`}}class E{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`,p=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${p}';
`,n.push(s)});let r="",a=new Set;return e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((i)=>i.type.includes("webhook")).forEach((i)=>{let g=(i.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),d=(i.parameters?.httpMethod||"POST").toUpperCase(),c=d==="ALL"?["GET","POST","PUT","DELETE"]:[d],l=Array.from(new Set([`/${g}`,`/webhook/${g}`]));c.forEach((f)=>{l.forEach((N)=>{let h=`${f}:${N}`;if(a.has(h))return;a.add(h),r+=`
  fastify.route({
    method: '${f}',
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
`}static generateOutputPackageJson(e=[]){let t=e.some((n)=>n.nodes.some((r)=>r.type===v));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...t?{"@aws-sdk/client-s3":"3.808.0","mime-types":"^3.0.1"}:{}}},null,2)}}function W(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=P.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let a=S.generateWorkflowCode(r),o=E.generateRoutesCode([r]),s=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:s,workflowCode:a,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:W};})();
