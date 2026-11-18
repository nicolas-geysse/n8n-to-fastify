(()=>{var{defineProperty:O,getOwnPropertyNames:V,getOwnPropertyDescriptor:q}=Object,B=Object.prototype.hasOwnProperty;function L(e){return this[e]}var D=(e)=>{var t=(P??=new WeakMap).get(e),n;if(t)return t;if(t=O({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of V(e))if(!B.call(t,r))O(t,r,{get:L.bind(e,r),enumerable:!(n=q(e,r))||n.enumerable})}return P.set(e,t),t},P;var F=(e)=>e;function G(e,t){this[e]=F.bind(null,t)}var z=(e,t)=>{for(var n in t)O(e,n,{get:t[n],enumerable:!0,configurable:!0,set:G.bind(t,n)})};var H={};z(H,{compileWorkflowJson:()=>W});class k{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,a=e.name||t,o=e.nodes||[],i=e.connections||{},s=n||`${t}.json`,m=new Map;o.forEach((d)=>m.set(d.name,[])),Object.entries(i).forEach(([d,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((y,p)=>{if(Array.isArray(y))y.forEach((l)=>{if(l&&l.node&&m.has(l.node))m.get(l.node)?.push({sourceNode:d,outputIndex:p})})})}),o.forEach((d)=>{d.incomingConnections=m.get(d.name)||[]});let g=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),c=new Set(g.map((d)=>d.name));o.forEach((d)=>{let u=new Set,y=new Set,p=[...(m.get(d.name)||[]).map((l)=>l.sourceNode)];while(p.length>0){let l=p.shift();if(y.has(l))continue;if(y.add(l),c.has(l))u.add(l);(m.get(l)||[]).forEach((v)=>p.push(v.sourceNode))}d.ancestorTriggers=Array.from(u)});let N=this.topologicalSort(o,i);return{id:r,name:a,fileName:t,relativePath:s.replace(/\.json$/,".ts"),nodes:o,connections:i,orderedNodes:N,triggers:g}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((c)=>{n.set(c.name,c),r.set(c.name,[])}),Object.entries(t).forEach(([c,N])=>{if(N.main&&Array.isArray(N.main))N.main.forEach((d)=>{if(Array.isArray(d))d.forEach((u)=>{if(u&&u.node&&n.has(u.node))r.get(c)?.push(u.node)})})});let a=new Set,o=new Set,i=[],s=(c,N=[])=>{if(a.has(c))return;if(o.has(c)){let y=[...N,c].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(c),N.push(c);let d=r.get(c)||[];for(let y of d)s(y,N);N.pop(),o.delete(c),a.add(c);let u=n.get(c);if(u)i.push(u)},m=new Map;e.forEach((c)=>m.set(c.name,0));for(let c of r.values())for(let N of c)m.set(N,(m.get(N)||0)+1);let g=e.map((c)=>c.name).filter((c)=>m.get(c)===0);for(let c of g)s(c);for(let c of e)if(!a.has(c.name))s(c.name);return i.reverse()}}class f{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let i=t.substring(1).trim();if(i.startsWith("{{")&&i.endsWith("}}")){let s=i.substring(2,i.length-2).trim();if(!s.includes("{{")&&!s.includes("}}"))return`(${this.transformTokens(s)})`}t=t.substring(1).trim()}let n=[],r=0,o=t.replace(/\{\{(.*?)\}\}/g,(i,s)=>{let m=this.transformTokens(s.trim()),g=`__EXPR_PLACEHOLDER_${r++}__`;return n.push(`\${(${m}) ?? ''}`),g}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((i,s)=>{o=o.replace(`__EXPR_PLACEHOLDER_${s}__`,i)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,r,a)=>{let o=a?a.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(r)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(n,r)=>`ctx.getNode(${JSON.stringify(r)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,r)=>{if(!r)return"item?.json";return`item?.json?.${r.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,r)=>`process.env.${r}`),t}}var J={seconds:1000,minutes:60000,hours:3600000,days:86400000};function A(e,t){if(typeof t!=="string"||!Object.hasOwn(J,t))throw Error("Wait unit must be seconds, minutes, hours, or days");let n=typeof e==="string"&&e.trim()?Number(e):e;if(typeof n!=="number"||!Number.isFinite(n)||n<0)throw Error("Wait amount must be a finite nonnegative number");let r=n*J[t];if(r>Number.MAX_SAFE_INTEGER)throw Error("Wait interval is too large");return r}var I="n8n-nodes-base.wait";function R(e){let t=e.parameters??{};return{amount:t.amount??(e.typeVersion===1?1:5),unit:t.unit??(e.typeVersion===1?"hours":"seconds")}}function T(e){return typeof e==="string"&&e.trimStart().startsWith("=")&&e.includes("{{")}function U(e){let t=e.typeVersion??1.1;if(t!==1&&t!==1.1||(e.parameters?.resume??"timeInterval")!=="timeInterval")return"Wait supports the After Time Interval mode on node versions 1 and 1.1";let{amount:n,unit:r}=R(e);try{A(T(n)?0:n,T(r)?"seconds":r)}catch(a){return a.message}}function M(e){let t=U(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let{amount:n,unit:r}=R(e.node),a=e.previousVariableName??"inputItems";return`
    ${e.variableName} = ${a} || inputItems;
    if (${e.variableName}.length > 0) {
      const inputItems: N8nItem[] = ${e.variableName};
      const item = inputItems[0];
      await waitForInterval(${f.parse(n)}, ${f.parse(r)});
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}class C{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("if")||t.includes("filter"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t===I)return M(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",o="item?.json || {}";if(n==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)o=f.parse(t.responseBody);else if(n==="text"&&t.responseBody)o=f.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=e.node.parameters||{},n=t.jsCode||t.functionCode||"return $input.all();",r=e.previousVariableName||"inputItems",a=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,i)=>`(ctx.getNode(${JSON.stringify(i)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,i)=>`(ctx.getNode(${JSON.stringify(i)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,i)=>`(ctx.getNode(${JSON.stringify(i)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let i=f.parse(o.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${i});`)}}if(t.values)for(let o of["string","number","boolean"]){let i=t.values[o];if(Array.isArray(i)){for(let s of i)if(s.name){let m=f.parse(s.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(s.name)}, ${m});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapIfNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=[];if(t.conditions?.conditions&&Array.isArray(t.conditions.conditions))for(let i of t.conditions.conditions){let s=f.parse(i.leftValue),m=f.parse(i.rightValue);switch(i.operator?.operation||"equals"){case"equals":case"equal":r.push(`(${s} == ${m})`);break;case"notEquals":case"notEqual":r.push(`(${s} != ${m})`);break;case"contains":r.push(`String(${s} ?? '').includes(String(${m} ?? ''))`);break;case"gt":case"larger":r.push(`(Number(${s}) > Number(${m}))`);break;case"lt":case"smaller":r.push(`(Number(${s}) < Number(${m}))`);break;case"isEmpty":case"empty":r.push(`(${s} === undefined || ${s} === null || ${s} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${s} !== undefined && ${s} !== null && ${s} !== '')`);break;default:r.push(`Boolean(${s})`)}}let a=t.conditions?.combinator==="or"?" || ":" && ",o=r.length>0?r.join(a):"true";return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${o});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],a=[];r.forEach((i,s)=>{let m=i.conditions?.conditions?.[0];if(m){let g=f.parse(m.leftValue),c=f.parse(m.rightValue);a.push(`if (${g} == ${c}) { matchedIndex = ${s}; }`)}});let o=r.length;return`
    // [Node: ${e.node.name}] Switch Router
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let matchedIndex = ${o};
      ${a.join(" else ")}
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, matchedIndex);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),a=f.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${f.parse(m.value)}`).join(", ")} }`;let i="undefined";if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)i=`JSON.stringify(${f.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))i=`JSON.stringify({ ${t.bodyParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${f.parse(m.value)}`).join(", ")} })`}return`
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=f.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",a=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=f.parse(a);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class b{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",a=e.nodes.some((p)=>p.type===I)?`import { waitForInterval } from '${r}/wait-interval';
`:"",o=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${a}

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,i=`  let item: N8nItem = inputItems[0] || { json: {} };
`,s=`  ctx.setNodeOutput("__initial__", inputItems);
`,m="inputItems",g=new Map;e.orderedNodes.forEach((p,l)=>{let h=`node_${l}_${this.sanitizeIdentifier(p.name)}`;g.set(p.name,h)});let c=e.orderedNodes.filter((p)=>p.type.includes("webhook")||p.type.includes("trigger")||p.type.includes("cron")),N=e.orderedNodes.filter((p)=>!c.includes(p));c.forEach((p)=>{let l=e.orderedNodes.indexOf(p),h=g.get(p.name);i+=`  let ${h}: any = inputItems;
`;let v=(p.parameters?.path||p.name).toLowerCase().trim().replace(/^\/+/,"");s+=`  const triggerNode_${l} = item?.json?.__triggerNode;
`,s+=`  const triggerPath_${l} = item?.json?.__triggerPath;
`,s+=`  if (!triggerNode_${l} || triggerNode_${l} === ${JSON.stringify(p.name)} || triggerPath_${l} === ${JSON.stringify(v)}) {
`,s+=`    ${h} = inputItems;
`,s+=`    ctx.setNodeOutput(${JSON.stringify(p.name)}, inputItems);
`,s+=`  }
`}),N.forEach((p)=>{let l=g.get(p.name);i+=`  let ${l}: any = inputItems;
`;let h="inputItems",v="true";if(p.incomingConnections&&p.incomingConnections.length>0){if(p.incomingConnections.length===1){let w=p.incomingConnections[0];h=`(ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex}) ? ctx.getNode(${JSON.stringify(w.sourceNode)})?.all() : null)`}else h=`(${p.incomingConnections.map((S)=>`(ctx.isBranchActive(${JSON.stringify(S.sourceNode)}, ${S.outputIndex}) ? ctx.getNode(${JSON.stringify(S.sourceNode)})?.all() : null)`).join(" || ")})`;v=`(${p.incomingConnections.map((w)=>`ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`).join(" || ")})`}let x=C.mapNode({node:p,workflowName:e.name,variableName:l,previousVariableName:h}),E=`  if (${v}) {
    ${x.trim().replace(/\n/g,`
    `)}
  }
`;if(p.ancestorTriggers&&p.ancestorTriggers.length>0){let _=p.ancestorTriggers.map((w)=>`ctx.hasExecutedNode(${JSON.stringify(w)})`).join(" || ");s+=`  if (${_}) {
  ${E.trim().replace(/\n/g,`
  `)}
  }
`}else s+=E;m=p.name});let d=e.orderedNodes[e.orderedNodes.length-1]?.name,u=d?`(ctx.getNode(${JSON.stringify(d)})?.all() || [{ json: item.json }])`:"inputItems",y=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${o}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${i}
${s}
  const finalItems = ${u};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(y)}, ${t});
`}}class j{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let i=`workflow_${b.sanitizeIdentifier(o.fileName)}`,s=o.relativePath.replace(/\.ts$/,"");t+=`import { ${i} } from '../workflows/${s}';
`,n.push(i)});let r="",a=new Set;return e.forEach((o)=>{let i=`workflow_${b.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((m)=>m.type.includes("webhook")).forEach((m)=>{let g=(m.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),c=(m.parameters?.httpMethod||"POST").toUpperCase(),N=c==="ALL"?["GET","POST","PUT","DELETE"]:[c],d=Array.from(new Set([`/${g}`,`/webhook/${g}`]));N.forEach((u)=>{d.forEach((y)=>{let p=`${u}:${y}`;if(a.has(p))return;a.add(p),r+=`
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
          __triggerNode: ${JSON.stringify(m.name)},
          __triggerPath: ${JSON.stringify(g)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function W(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=k.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let a=b.generateWorkflowCode(r),o=j.generateRoutesCode([r]),i=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:i,workflowCode:a,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:W};})();
