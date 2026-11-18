(()=>{var{defineProperty:I,getOwnPropertyNames:J,getOwnPropertyDescriptor:A}=Object,T=Object.prototype.hasOwnProperty;function R(e){return this[e]}var M=(e)=>{var t=(P??=new WeakMap).get(e),r;if(t)return t;if(t=I({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of J(e))if(!T.call(t,n))I(t,n,{get:R.bind(e,n),enumerable:!(r=A(e,n))||r.enumerable})}return P.set(e,t),t},P;var W=(e)=>e;function q(e,t){this[e]=W.bind(null,t)}var V=(e,t)=>{for(var r in t)I(e,r,{get:t[r],enumerable:!0,configurable:!0,set:q.bind(t,r)})};var L={};V(L,{compileWorkflowJson:()=>_});class S{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,i=e.name||t,o=e.nodes||[],s=e.connections||{},c=r||`${t}.json`,m=new Map;o.forEach((d)=>m.set(d.name,[])),Object.entries(s).forEach(([d,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((a,g)=>{if(Array.isArray(a))a.forEach((l)=>{if(l&&l.node&&m.has(l.node))m.get(l.node)?.push({sourceNode:d,outputIndex:g})})})}),o.forEach((d)=>{d.incomingConnections=m.get(d.name)||[]});let y=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),p=new Set(y.map((d)=>d.name));o.forEach((d)=>{let f=new Set,a=new Set,g=[...(m.get(d.name)||[]).map((l)=>l.sourceNode)];while(g.length>0){let l=g.shift();if(a.has(l))continue;if(a.add(l),p.has(l))f.add(l);(m.get(l)||[]).forEach((v)=>g.push(v.sourceNode))}d.ancestorTriggers=Array.from(f)});let u=this.topologicalSort(o,s);return{id:n,name:i,fileName:t,relativePath:c.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:u,triggers:y}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((p)=>{r.set(p.name,p),n.set(p.name,[])}),Object.entries(t).forEach(([p,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((d)=>{if(Array.isArray(d))d.forEach((f)=>{if(f&&f.node&&r.has(f.node))n.get(p)?.push(f.node)})})});let i=new Set,o=new Set,s=[],c=(p,u=[])=>{if(i.has(p))return;if(o.has(p)){let a=[...u,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${a}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(p),u.push(p);let d=n.get(p)||[];for(let a of d)c(a,u);u.pop(),o.delete(p),i.add(p);let f=r.get(p);if(f)s.push(f)},m=new Map;e.forEach((p)=>m.set(p.name,0));for(let p of n.values())for(let u of p)m.set(u,(m.get(u)||0)+1);let y=e.map((p)=>p.name).filter((p)=>m.get(p)===0);for(let p of y)c(p);for(let p of e)if(!i.has(p.name))c(p.name);return s.reverse()}}class N{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let c=s.substring(2,s.length-2).trim();if(!c.includes("{{")&&!c.includes("}}"))return`(${this.transformTokens(c)})`}t=t.substring(1).trim()}let r=[],n=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,c)=>{let m=this.transformTokens(c.trim()),y=`__EXPR_PLACEHOLDER_${n++}__`;return r.push(`\${(${m}) ?? ''}`),y}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return r.forEach((s,c)=>{o=o.replace(`__EXPR_PLACEHOLDER_${c}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(r,n,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(n)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(r,n,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(n)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(r,n)=>`ctx.getNode(${JSON.stringify(n)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(r,n)=>`ctx.getNode(${JSON.stringify(n)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(r,n)=>{if(!n)return"item?.json";return`item?.json?.${n.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(r,n)=>`process.env.${n}`),t}}class O{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",o="item?.json || {}";if(r==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)o=N.parse(t.responseBody);else if(r==="text"&&t.responseBody)o=N.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${i} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${o};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${n}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},r=t.jsCode||t.functionCode||"return $input.all();",n=e.previousVariableName||"inputItems",i=r.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,s)=>`(ctx.getNode(${JSON.stringify(s)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${n} || inputItems;
      const result = await (async () => {
        ${i}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},r=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(r)}, ${n} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=N.parse(o.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let c of s)if(c.name){let m=N.parse(c.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(c.name)}, ${m});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[];if(Array.isArray(e?.conditions))for(let i of e.conditions){let o=N.parse(i.leftValue),s=N.parse(i.rightValue),c=i.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(c)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&i.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,s=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${s}))`;switch(c){case"equals":case"equal":r.push(`(${o} == ${s})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${s})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${s} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${s}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${s}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${s}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${s}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let n=e?.combinator==="or"?" || ":" && ";return r.length?r.join(n):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
    // [Node: ${e.node.name}] Filter
    ${e.variableName} = ${t} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${r});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapIfNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions);return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${t} || inputItems;
    {
      const outputs: N8nItem[][] = [[], []];
      for (const item of ${e.variableName} as N8nItem[]) {
        const port = (${r}) ? 0 : 1;
        outputs[port].push(item);
      }
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs.entries(), ${e.variableName});
    }
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.rules?.values||[],i=t.options??{},o=i.fallbackOutput??"none",s=o==="extra"?n.length:o,m=n.length>0&&Number.isInteger(s)&&s>=0&&(o==="extra"||s<n.length)?`
        if (!matched) {
          send(item, ${s});
          matched = true;
        }`:"",p=`
      const predicates = [${n.map((d)=>`(item: N8nItem) => (${d.conditions?this.parseConditions(d.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${N.parse(i.allMatchingOutputs??!1)})) break;
          }
        }
        ${m}
        if (matched) routed.push(item);
      }`,u=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${N.parse(t.numberOutputs??4)}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${N.parse(t.output??0)};
        if (!Number.isSafeInteger(numberOutputs) || !Number.isSafeInteger(port) || port < 0 || port >= numberOutputs) {
          throw new Error('Switch expression returned an invalid output index');
        }
        send(item, port);
        routed.push(item);
      }
    `;return`
    // [Node: ${e.node.name}] Switch Router
    {
      const source: N8nItem[] = ${r} || inputItems;
      const outputs = new Map<number, N8nItem[]>();
      const routed: N8nItem[] = [];
      const send = (item: N8nItem, port: number) => {
        let items = outputs.get(port);
        if (!items) {
          items = [];
          outputs.set(port, items);
        }
        items.push(item);
      };
      ${t.mode==="expression"?u:p}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),i=N.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${N.parse(m.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)s=`JSON.stringify(${N.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((m)=>m.name).map((m)=>`${JSON.stringify(m.name)}: ${N.parse(m.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${n})
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${o} };
      const reqBody = ${s};
      const res = await fetch(targetUrl, {
        method: ${JSON.stringify(n)},
        headers: reqHeaders,
        ...(reqBody !== undefined ? { body: typeof reqBody === 'string' ? reqBody : JSON.stringify(reqBody) } : {})
      });
      const text = await res.text();
      let data: any;
      try { data = JSON.parse(text); } catch { data = { raw: text }; }
      return { json: typeof data === 'object' && data !== null ? data : { data } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapAggregateNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.destinationFieldName||"data";return`
    // [Node: ${e.node.name}] Aggregate Items
    {
      const source = ${r} || inputItems;
      ${e.variableName} = [{ json: { ${JSON.stringify(n)}: source.map((i: N8nItem) => i.json) } }];
      item = ${e.variableName}[0];
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapSplitOutNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.fieldToSplitOut||"data";return`
    // [Node: ${e.node.name}] Split Out Array Field
    {
      const source = ${r} || inputItems;
      const expanded: N8nItem[] = [];
      for (const entry of source) {
        const arr = entry.json?.[${JSON.stringify(n)}];
        if (Array.isArray(arr)) {
          for (const el of arr) {
            expanded.push({ json: typeof el === 'object' && el !== null ? el : { [${JSON.stringify(n)}]: el } });
          }
        }
      }
      ${e.variableName} = expanded.length > 0 ? expanded : source;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],r=e.node.parameters?.mode||"append",n=t.map((i)=>`(ctx.getNodeOutput(${JSON.stringify(i.sourceNode)}, ${i.outputIndex}) || [])`);if(r==="combine"&&n.length>=2)return`
    // [Node: ${e.node.name}] Merge (Combine Mode)
    {
      const left = ${n[0]};
      const right = ${n[1]};
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
      const combined = [${n.join(", ")}].flat();
      ${e.variableName} = combined.length > 0 ? combined : inputItems;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=N.parse(t.query||"SELECT 1;");return`
    // [Node: ${e.node.name}] PostgreSQL Query (via DATABASE_URL env var)
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const sql = ${n};
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=N.parse(i);return`
    // [Node: ${e.node.name}] Native LLM Completion (OpenAI-compatible REST API)
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
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
          model: ${JSON.stringify(n)},
          messages: [{ role: 'user', content: String(prompt) }]
        })
      });
      const data: any = await res.json();
      const text = data?.choices?.[0]?.message?.content || '';
      return { json: { text, model: ${JSON.stringify(n)}, usage: data?.usage } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=Number(t.amount||1),i=t.unit||"seconds",s=n*(i==="minutes"?60000:i==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${n} ${i})
    await new Promise((resolve) => setTimeout(resolve, ${s}));
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class b{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",i=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,o=`  let item: N8nItem = inputItems[0] || { json: {} };
`,s=`  ctx.setNodeOutput("__initial__", inputItems);
`,c="inputItems",m=new Map;e.orderedNodes.forEach((a,g)=>{let l=`node_${g}_${this.sanitizeIdentifier(a.name)}`;m.set(a.name,l)});let y=e.orderedNodes.filter((a)=>a.type.includes("webhook")||a.type.includes("trigger")||a.type.includes("cron")),p=e.orderedNodes.filter((a)=>!y.includes(a));y.forEach((a)=>{let g=e.orderedNodes.indexOf(a),l=m.get(a.name);o+=`  let ${l}: any = inputItems;
`;let w=(a.parameters?.path||a.name).toLowerCase().trim().replace(/^\/+/,"");s+=`  const triggerNode_${g} = item?.json?.__triggerNode;
`,s+=`  const triggerPath_${g} = item?.json?.__triggerPath;
`,s+=`  if (!triggerNode_${g} || triggerNode_${g} === ${JSON.stringify(a.name)} || triggerPath_${g} === ${JSON.stringify(w)}) {
`,s+=`    ${l} = inputItems;
`,s+=`    ctx.setNodeOutput(${JSON.stringify(a.name)}, inputItems);
`,s+=`  }
`}),p.forEach((a)=>{let g=m.get(a.name);o+=`  let ${g}: any = inputItems;
`;let l="inputItems",w="true";if(a.incomingConnections&&a.incomingConnections.length>0){if(a.incomingConnections.length===1){let h=a.incomingConnections[0];l=`ctx.getNodeOutput(${JSON.stringify(h.sourceNode)}, ${h.outputIndex})`}else l=`(${a.incomingConnections.map((E)=>`ctx.getNodeOutput(${JSON.stringify(E.sourceNode)}, ${E.outputIndex})`).join(" || ")})`;w=`(${a.incomingConnections.map((h)=>`ctx.isBranchActive(${JSON.stringify(h.sourceNode)}, ${h.outputIndex})`).join(" || ")})`}let v=O.mapNode({node:a,workflowName:e.name,variableName:g,previousVariableName:l}),C=`  if (${w}) {
    ${v.trim().replace(/\n/g,`
    `)}
  }
`;if(a.ancestorTriggers&&a.ancestorTriggers.length>0){let j=a.ancestorTriggers.map((h)=>`ctx.hasExecutedNode(${JSON.stringify(h)})`).join(" || ");s+=`  if (${j}) {
  ${C.trim().replace(/\n/g,`
  `)}
  }
`}else s+=C;c=a.name});let u=e.orderedNodes[e.orderedNodes.length-1]?.name,d=u?`(ctx.getNode(${JSON.stringify(u)})?.all() || [{ json: item.json }])`:"inputItems",f=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${i}
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

registerWorkflow(${JSON.stringify(f)}, ${t});
`}}class k{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((o)=>{let s=`workflow_${b.sanitizeIdentifier(o.fileName)}`,c=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${c}';
`,r.push(s)});let n="",i=new Set;return e.forEach((o)=>{let s=`workflow_${b.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((m)=>m.type.includes("webhook")).forEach((m)=>{let y=(m.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),p=(m.parameters?.httpMethod||"POST").toUpperCase(),u=p==="ALL"?["GET","POST","PUT","DELETE"]:[p],d=Array.from(new Set([`/${y}`,`/webhook/${y}`]));u.forEach((f)=>{d.forEach((a)=>{let g=`${f}:${a}`;if(i.has(g))return;i.add(g),n+=`
  fastify.route({
    method: '${f}',
    url: '${a}',
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
const _registeredWorkflows = [${r.join(", ")}];
void _registeredWorkflows;

export async function registerCompiledRoutes(fastify: FastifyInstance): Promise<void> {
${n}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function _(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=S.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=b.generateWorkflowCode(n),o=k.generateRoutesCode([n]),s=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:s,workflowCode:i,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:_};})();
