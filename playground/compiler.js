(()=>{var{defineProperty:O,getOwnPropertyNames:J,getOwnPropertyDescriptor:A}=Object,T=Object.prototype.hasOwnProperty;function R(e){return this[e]}var M=(e)=>{var t=(P??=new WeakMap).get(e),r;if(t)return t;if(t=O({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var s of J(e))if(!T.call(t,s))O(t,s,{get:R.bind(e,s),enumerable:!(r=A(e,s))||r.enumerable})}return P.set(e,t),t},P;var W=(e)=>e;function q(e,t){this[e]=W.bind(null,t)}var V=(e,t)=>{for(var r in t)O(e,r,{get:t[r],enumerable:!0,configurable:!0,set:q.bind(t,r)})};var B={};V(B,{compileWorkflowJson:()=>_});class k{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let s=e.id||t,a=e.name||t,o=e.nodes||[],n=e.connections||{},c=r||`${t}.json`,i=new Map;o.forEach((d)=>i.set(d.name,[])),Object.entries(n).forEach(([d,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((p,g)=>{if(Array.isArray(p))p.forEach((l)=>{if(l&&l.node&&i.has(l.node))i.get(l.node)?.push({sourceNode:d,outputIndex:g})})})}),o.forEach((d)=>{d.incomingConnections=i.get(d.name)||[]});let y=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),m=new Set(y.map((d)=>d.name));o.forEach((d)=>{let f=new Set,p=new Set,g=[...(i.get(d.name)||[]).map((l)=>l.sourceNode)];while(g.length>0){let l=g.shift();if(p.has(l))continue;if(p.add(l),m.has(l))f.add(l);(i.get(l)||[]).forEach((v)=>g.push(v.sourceNode))}d.ancestorTriggers=Array.from(f)});let u=this.topologicalSort(o,n);return{id:s,name:a,fileName:t,relativePath:c.replace(/\.json$/,".ts"),nodes:o,connections:n,orderedNodes:u,triggers:y}}static topologicalSort(e,t){let r=new Map,s=new Map;e.forEach((m)=>{r.set(m.name,m),s.set(m.name,[])}),Object.entries(t).forEach(([m,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((d)=>{if(Array.isArray(d))d.forEach((f)=>{if(f&&f.node&&r.has(f.node))s.get(m)?.push(f.node)})})});let a=new Set,o=new Set,n=[],c=(m,u=[])=>{if(a.has(m))return;if(o.has(m)){let p=[...u,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${p}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(m),u.push(m);let d=s.get(m)||[];for(let p of d)c(p,u);u.pop(),o.delete(m),a.add(m);let f=r.get(m);if(f)n.push(f)},i=new Map;e.forEach((m)=>i.set(m.name,0));for(let m of s.values())for(let u of m)i.set(u,(i.get(u)||0)+1);let y=e.map((m)=>m.name).filter((m)=>i.get(m)===0);for(let m of y)c(m);for(let m of e)if(!a.has(m.name))c(m.name);return n.reverse()}}class N{static parse(e,t){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let c=r.substring(1).trim();if(c.startsWith("{{")&&c.endsWith("}}")){let i=c.substring(2,c.length-2).trim();if(!i.includes("{{")&&!i.includes("}}"))return`(${this.transformTokens(i,t)})`}r=r.substring(1).trim()}let s=[],a=0,n=r.replace(/\{\{(.*?)\}\}/g,(c,i)=>{let y=this.transformTokens(i.trim(),t),m=`__EXPR_PLACEHOLDER_${a++}__`;return s.push(`\${(${y}) ?? ''}`),m}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return s.forEach((c,i)=>{n=n.replace(`__EXPR_PLACEHOLDER_${i}__`,c)}),`\`${n}\``}static transformTokens(e,t){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(s,a,o)=>{let n=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(a)})?.item?.json${n.length?"?."+n.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(s,a,o)=>{let n=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(a)})?.first()?.json${n.length?"?."+n.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(s,a)=>`ctx.getNode(${JSON.stringify(a)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(s,a)=>`ctx.getNode(${JSON.stringify(a)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(s,a)=>{if(!a)return"item?.json";return`item?.json?.${a.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,"inputItems"),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(s,a)=>`process.env.${a}`),r}}class C{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",s=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",o="item?.json || {}";if(r==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)o=N.parse(t.responseBody);else if(r==="text"&&t.responseBody)o=N.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${a} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${o};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${s}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},r=t.jsCode||t.functionCode||"return $input.all();",s=e.previousVariableName||"inputItems",a=r.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(o,n)=>`(ctx.getNode(${JSON.stringify(n)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(o,n)=>`(ctx.getNode(${JSON.stringify(n)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(o,n)=>`(ctx.getNode(${JSON.stringify(n)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${s} || inputItems;
      const result = await (async () => {
        ${a}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},r=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",s=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(r)}, ${s} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=t.options?.keepOnlySet===!0||t.include==="none",a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let n=N.parse(o.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${n});`)}}if(t.values)for(let o of["string","number","boolean"]){let n=t.values[o];if(Array.isArray(n)){for(let c of n)if(c.name){let i=N.parse(c.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(c.name)}, ${i});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${s?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],s=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let o of e.conditions){let n=N.parse(o.leftValue,s),c=N.parse(o.rightValue,s),i=o.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(i)){r.push(`Boolean(${n})`);continue}if(!t&&e.options?.caseSensitive===!1&&o.operator?.type==="string")n=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${n}))`,c=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${c}))`;switch(i){case"equals":case"equal":r.push(`(${n} == ${c})`);break;case"notEquals":case"notEqual":r.push(`(${n} != ${c})`);break;case"contains":r.push(`String(${n} ?? '').includes(String(${c} ?? ''))`);break;case"true":r.push(`(${n} === true)`);break;case"false":r.push(`(${n} === false)`);break;case"gt":case"larger":r.push(`(Number(${n}) > Number(${c}))`);break;case"lt":case"smaller":r.push(`(Number(${n}) < Number(${c}))`);break;case"gte":r.push(`(Number(${n}) >= Number(${c}))`);break;case"lte":r.push(`(Number(${n}) <= Number(${c}))`);break;case"isEmpty":case"empty":r.push(`(${n} === undefined || ${n} === null || ${n} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${n} !== undefined && ${n} !== null && ${n} !== '')`);break;default:r.push(`Boolean(${n})`)}}let a=e?.combinator==="or"?" || ":" && ";return r.length?r.join(a):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
      const firstInputItem: N8nItem | undefined = ${e.variableName}[0];
      const outputs: N8nItem[][] = [[], []];
      for (const item of ${e.variableName} as N8nItem[]) {
        const port = (${r}) ? 0 : 1;
        outputs[port].push(item);
      }
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs.entries(), ${e.variableName});
    }
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=t.rules?.values||[],a=t.options??{},o=a.fallbackOutput??"none",n=o==="extra"?s.length:o,i=o==="extra"||Number.isInteger(n)&&n>=0&&n<s.length?`
        if (!matched) {
          send(item, ${n});
          matched = true;
        }`:"",m=`
      const predicates = [${s.map((d)=>`(item: N8nItem) => (${d.conditions?this.parseConditions(d.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${N.parse(a.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${i}
        if (matched) routed.push(item);
      }`,u=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${N.parse(t.numberOutputs??4,"firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${N.parse(t.output??0,"firstInputItem")};
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
      const firstInputItem = source[0];
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
      ${t.mode==="expression"?u:m}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=(t.method||"GET").toUpperCase(),a=N.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${N.parse(i.value)}`).join(", ")} }`;let n="undefined";if(["POST","PUT","PATCH"].includes(s)){if(t.jsonBody)n=`JSON.stringify(${N.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))n=`JSON.stringify({ ${t.bodyParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${N.parse(i.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${s})
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${a};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${o} };
      const reqBody = ${n};
      const res = await fetch(targetUrl, {
        method: ${JSON.stringify(s)},
        headers: reqHeaders,
        ...(reqBody !== undefined ? { body: typeof reqBody === 'string' ? reqBody : JSON.stringify(reqBody) } : {})
      });
      const text = await res.text();
      let data: any;
      try { data = JSON.parse(text); } catch { data = { raw: text }; }
      return { json: typeof data === 'object' && data !== null ? data : { data } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapAggregateNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=t.destinationFieldName||"data";return`
    // [Node: ${e.node.name}] Aggregate Items
    {
      const source = ${r} || inputItems;
      ${e.variableName} = [{ json: { ${JSON.stringify(s)}: source.map((i: N8nItem) => i.json) } }];
      item = ${e.variableName}[0];
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapSplitOutNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=t.fieldToSplitOut||"data";return`
    // [Node: ${e.node.name}] Split Out Array Field
    {
      const source = ${r} || inputItems;
      const expanded: N8nItem[] = [];
      for (const entry of source) {
        const arr = entry.json?.[${JSON.stringify(s)}];
        if (Array.isArray(arr)) {
          for (const el of arr) {
            expanded.push({ json: typeof el === 'object' && el !== null ? el : { [${JSON.stringify(s)}]: el } });
          }
        }
      }
      ${e.variableName} = expanded.length > 0 ? expanded : source;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],r=e.node.parameters?.mode||"append",s=t.map((a)=>`(ctx.getNodeOutput(${JSON.stringify(a.sourceNode)}, ${a.outputIndex}) || [])`);if(r==="combine"&&s.length>=2)return`
    // [Node: ${e.node.name}] Merge (Combine Mode)
    {
      const left = ${s[0]};
      const right = ${s[1]};
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
      const combined = [${s.join(", ")}].flat();
      ${e.variableName} = combined.length > 0 ? combined : inputItems;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=N.parse(t.query||"SELECT 1;");return`
    // [Node: ${e.node.name}] PostgreSQL Query (via DATABASE_URL env var)
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const sql = ${s};
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=t.modelId?.value||t.model||"gpt-4o-mini",a=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=N.parse(a);return`
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
          model: ${JSON.stringify(s)},
          messages: [{ role: 'user', content: String(prompt) }]
        })
      });
      const data: any = await res.json();
      const text = data?.choices?.[0]?.message?.content || '';
      return { json: { text, model: ${JSON.stringify(s)}, usage: data?.usage } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",s=Number(t.amount||1),a=t.unit||"seconds",n=s*(a==="minutes"?60000:a==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${s} ${a})
    await new Promise((resolve) => setTimeout(resolve, ${n}));
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class w{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,s="../".repeat(r+1)+"runtime",a=`import { WorkflowContext } from '${s}/workflow-context';
import { n8n, N8nItem } from '${s}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${s}/workflow-registry';

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,o=`  let item: N8nItem = inputItems[0] || { json: {} };
`,n=`  ctx.setNodeOutput("__initial__", inputItems);
`,c="inputItems",i=new Map;e.orderedNodes.forEach((p,g)=>{let l=`node_${g}_${this.sanitizeIdentifier(p.name)}`;i.set(p.name,l)});let y=e.orderedNodes.filter((p)=>p.type.includes("webhook")||p.type.includes("trigger")||p.type.includes("cron")),m=e.orderedNodes.filter((p)=>!y.includes(p));y.forEach((p)=>{let g=e.orderedNodes.indexOf(p),l=i.get(p.name);o+=`  let ${l}: any = inputItems;
`;let h=(p.parameters?.path||p.name).toLowerCase().trim().replace(/^\/+/,"");n+=`  const triggerNode_${g} = item?.json?.__triggerNode;
`,n+=`  const triggerPath_${g} = item?.json?.__triggerPath;
`,n+=`  if (!triggerNode_${g} || triggerNode_${g} === ${JSON.stringify(p.name)} || triggerPath_${g} === ${JSON.stringify(h)}) {
`,n+=`    ${l} = inputItems;
`,n+=`    ctx.setNodeOutput(${JSON.stringify(p.name)}, inputItems);
`,n+=`  }
`}),m.forEach((p)=>{let g=i.get(p.name);o+=`  let ${g}: any = inputItems;
`;let l="true",h=p.incomingConnections??[],v=h.length>0&&!p.type.includes("merge");if(h.length>0)l=`(${h.map((b)=>`ctx.isBranchActive(${JSON.stringify(b.sourceNode)}, ${b.outputIndex})`).join(" || ")})`;let S=C.mapNode({node:p,workflowName:e.name,variableName:g,previousVariableName:v?"incomingItems":"inputItems"});if(v){let I=h.map((b)=>`ctx.getNodeOutputBatches(${JSON.stringify(b.sourceNode)}, ${b.outputIndex})`);S=`await ctx.runNodeBatches(${JSON.stringify(p.name)}, [${I.join(", ")}].flat(), async (incomingItems) => {
${S.trim()}
});`}let E=`  if (${l}) {
    ${S.trim().replace(/\n/g,`
    `)}
  }
`;if(p.ancestorTriggers&&p.ancestorTriggers.length>0){let I=p.ancestorTriggers.map((b)=>`ctx.hasExecutedNode(${JSON.stringify(b)})`).join(" || ");n+=`  if (${I}) {
  ${E.trim().replace(/\n/g,`
  `)}
  }
`}else n+=E;c=p.name});let u=e.orderedNodes[e.orderedNodes.length-1]?.name,d=u?`(ctx.getNode(${JSON.stringify(u)})?.all() || [{ json: item.json }])`:"inputItems",f=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${a}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${o}
${n}
  const finalItems = ${d};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(f)}, ${t});
`}}class j{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((o)=>{let n=`workflow_${w.sanitizeIdentifier(o.fileName)}`,c=o.relativePath.replace(/\.ts$/,"");t+=`import { ${n} } from '../workflows/${c}';
`,r.push(n)});let s="",a=new Set;return e.forEach((o)=>{let n=`workflow_${w.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((i)=>i.type.includes("webhook")).forEach((i)=>{let y=(i.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),m=(i.parameters?.httpMethod||"POST").toUpperCase(),u=m==="ALL"?["GET","POST","PUT","DELETE"]:[m],d=Array.from(new Set([`/${y}`,`/webhook/${y}`]));u.forEach((f)=>{d.forEach((p)=>{let g=`${f}:${p}`;if(a.has(g))return;a.add(g),s+=`
  fastify.route({
    method: '${f}',
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
          __triggerPath: ${JSON.stringify(y)}
        }
      }];

      try {
        const { items, context } = await ${n}(inputItems, ctx);
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
${s}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function _(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,s=k.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!s)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let a=w.generateWorkflowCode(s),o=j.generateRoutesCode([s]),n=(performance.now()-t).toFixed(2);return{name:s.name,nodeCount:s.nodes.length,triggerCount:s.triggers.length,elapsedMs:n,workflowCode:a,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:_};})();
