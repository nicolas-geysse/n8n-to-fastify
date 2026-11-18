(()=>{var{defineProperty:C,getOwnPropertyNames:L,getOwnPropertyDescriptor:F}=Object,D=Object.prototype.hasOwnProperty;function G(e){return this[e]}var z=(e)=>{var t=(T??=new WeakMap).get(e),r;if(t)return t;if(t=C({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of L(e))if(!D.call(t,n))C(t,n,{get:G.bind(e,n),enumerable:!(r=F(e,n))||r.enumerable})}return T.set(e,t),t},T;var U=(e)=>e;function H(e,t){this[e]=U.bind(null,t)}var K=(e,t)=>{for(var r in t)C(e,r,{get:t[r],enumerable:!0,configurable:!0,set:H.bind(t,r)})};var Q={};K(Q,{compileWorkflowJson:()=>B});class j{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},c=r||`${t}.json`,a=new Map;s.forEach((d)=>a.set(d.name,[])),Object.entries(o).forEach(([d,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((N,h)=>{if(Array.isArray(N))N.forEach((m)=>{if(m&&m.node&&a.has(m.node))a.get(m.node)?.push({sourceNode:d,outputIndex:h})})})}),s.forEach((d)=>{d.incomingConnections=a.get(d.name)||[]});let g=s.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),p=new Set(g.map((d)=>d.name));s.forEach((d)=>{let f=new Set,N=new Set,h=[...(a.get(d.name)||[]).map((m)=>m.sourceNode)];while(h.length>0){let m=h.shift();if(N.has(m))continue;if(N.add(m),p.has(m))f.add(m);(a.get(m)||[]).forEach((w)=>h.push(w.sourceNode))}d.ancestorTriggers=Array.from(f)});let l=this.topologicalSort(s,o);return{id:n,name:i,fileName:t,relativePath:c.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:l,triggers:g}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((p)=>{r.set(p.name,p),n.set(p.name,[])}),Object.entries(t).forEach(([p,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((d)=>{if(Array.isArray(d))d.forEach((f)=>{if(f&&f.node&&r.has(f.node))n.get(p)?.push(f.node)})})});let i=new Set,s=new Set,o=[],c=(p,l=[])=>{if(i.has(p))return;if(s.has(p)){let N=[...l,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${N}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(p),l.push(p);let d=n.get(p)||[];for(let N of d)c(N,l);l.pop(),s.delete(p),i.add(p);let f=r.get(p);if(f)o.push(f)},a=new Map;e.forEach((p)=>a.set(p.name,0));for(let p of n.values())for(let l of p)a.set(l,(a.get(l)||0)+1);let g=e.map((p)=>p.name).filter((p)=>a.get(p)===0);for(let p of g)c(p);for(let p of e)if(!i.has(p.name))c(p.name);return o.reverse()}}class u{static parse(e,t){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let c=r.substring(1).trim();if(c.startsWith("{{")&&c.endsWith("}}")){let a=c.substring(2,c.length-2).trim();if(!a.includes("{{")&&!a.includes("}}"))return`(${this.transformTokens(a,t)})`}r=r.substring(1).trim()}let n=[],i=0,o=r.replace(/\{\{(.*?)\}\}/g,(c,a)=>{let g=this.transformTokens(a.trim(),t),p=`__EXPR_PLACEHOLDER_${i++}__`;return n.push(`\${(${g}) ?? ''}`),p}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((c,a)=>{o=o.replace(`__EXPR_PLACEHOLDER_${a}__`,c)}),`\`${o}\``}static transformTokens(e,t){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.item?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,i)=>{if(!i)return"item?.json";return`item?.json?.${i.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,"inputItems"),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,i)=>`process.env.${i}`),r}}var M={seconds:1000,minutes:60000,hours:3600000,days:86400000};function R(e,t){if(typeof t!=="string"||!Object.hasOwn(M,t))throw Error("Wait unit must be seconds, minutes, hours, or days");let r=typeof e==="string"&&e.trim()?Number(e):e;if(typeof r!=="number"||!Number.isFinite(r)||r<0)throw Error("Wait amount must be a finite nonnegative number");let n=r*M[t];if(n>Number.MAX_SAFE_INTEGER)throw Error("Wait interval is too large");return n}var O="n8n-nodes-base.wait";function x(e){let t=e.parameters??{};return{amount:t.amount??(e.typeVersion===1?1:5),unit:t.unit??(e.typeVersion===1?"hours":"seconds")}}function W(e){return typeof e==="string"&&e.trimStart().startsWith("=")&&e.includes("{{")}function X(e){let t=e.typeVersion??1.1;if(t!==1&&t!==1.1||(e.parameters?.resume??"timeInterval")!=="timeInterval")return"Wait supports the After Time Interval mode on node versions 1 and 1.1";let{amount:r,unit:n}=x(e);try{R(W(r)?0:r,W(n)?"seconds":n)}catch(i){return i.message}}function V(e){let t=X(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let{amount:r,unit:n}=x(e.node),i=e.previousVariableName??"inputItems";return`
    ${e.variableName} = ${i} || inputItems;
    if (${e.variableName}.length > 0) {
      const inputItems: N8nItem[] = ${e.variableName};
      const item = inputItems[0];
      await waitForInterval(${u.parse(r)}, ${u.parse(n)});
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}var k="n8n-nodes-base.stopAndError";function Z(e){if((e.typeVersion??1)!==1)return"Stop and Error supports node version 1";let t=e.parameters?.errorType??"errorMessage";if(t!=="errorMessage"&&t!=="errorObject")return"Stop and Error supports Error Message and Error Object"}function q(e){let t=Z(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters??{},n=r.errorType??"errorMessage",i=r[n]??"",s=u.parse(typeof i==="string"&&!i.includes("{{")?i.replace(/^=/,""):i);return`
    {
      const source: N8nItem[] = ${e.previousVariableName??"inputItems"} || inputItems;
      if (source.length > 0) {
        const inputItems = source;
        const item = inputItems[0];
        throw createWorkflowStopError(${JSON.stringify(n)}, ${s});
      }
    }`}class _{static mapNode(e){let t=e.node.type;if(t===k)return q(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t===O)return V(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(r==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)s=u.parse(t.responseBody);else if(r==="text"&&t.responseBody)s=u.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${i} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${s};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${n}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=e.node.parameters||{},r=t.jsCode||t.functionCode||"return $input.all();",n=e.previousVariableName||"inputItems",i=r.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let s of t.assignments.assignments)if(s.name){let o=u.parse(s.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(s.name)}, ${o});`)}}if(t.values)for(let s of["string","number","boolean"]){let o=t.values[s];if(Array.isArray(o)){for(let c of o)if(c.name){let a=u.parse(c.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(c.name)}, ${a});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],n=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=u.parse(s.leftValue,n),c=u.parse(s.rightValue,n),a=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(a)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,c=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${c}))`;switch(a){case"equals":case"equal":r.push(`(${o} == ${c})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${c})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${c} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${c}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${c}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${c}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${c}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let i=e?.combinator==="or"?" || ":" && ";return r.length?r.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.rules?.values||[],i=t.options??{},s=i.fallbackOutput??"none",o=s==="extra"?n.length:s,a=s==="extra"||Number.isInteger(o)&&o>=0&&o<n.length?`
        if (!matched) {
          send(item, ${o});
          matched = true;
        }`:"",p=`
      const predicates = [${n.map((d)=>`(item: N8nItem) => (${d.conditions?this.parseConditions(d.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${u.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${a}
        if (matched) routed.push(item);
      }`,l=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${u.parse(t.numberOutputs??4,"firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${u.parse(t.output??0,"firstInputItem")};
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
      ${t.mode==="expression"?l:p}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),i=u.parse(t.url||""),s="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))s=`{ ${t.headerParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${u.parse(a.value)}`).join(", ")} }`;let o="undefined";if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)o=`JSON.stringify(${u.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))o=`JSON.stringify({ ${t.bodyParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${u.parse(a.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${n})
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${s} };
      const reqBody = ${o};
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=u.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=u.parse(i);return`
    // [Node: ${e.node.name}] Native LLM Completion (OpenAI-compatible REST API)
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const apiKey = process.env.OPENAI_API_KEY || process.env.LLM_API_KEY || '';
      const baseUrl = process.env.LLM_BASE_URL || 'https://api.openai.com/v1';
      const prompt = ${s};
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class I{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",i=e.nodes.some((m)=>m.type===O)?`import { waitForInterval } from '${n}/wait-interval';
`:"",s=e.nodes.some((m)=>m.type===k)?`import { createWorkflowStopError } from '${n}/stop-and-error';
`:"",o=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${i}${s}

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,c=`  let item: N8nItem = inputItems[0] || { json: {} };
`,a=`  ctx.setNodeOutput("__initial__", inputItems);
`,g="inputItems",p=new Map;e.orderedNodes.forEach((m,y)=>{let w=`node_${y}_${this.sanitizeIdentifier(m.name)}`;p.set(m.name,w)});let l=e.orderedNodes.filter((m)=>m.type.includes("webhook")||m.type.includes("trigger")||m.type.includes("cron")),d=e.orderedNodes.filter((m)=>!l.includes(m));l.forEach((m)=>{let y=e.orderedNodes.indexOf(m),w=p.get(m.name);c+=`  let ${w}: any = inputItems;
`;let v=(m.parameters?.path||m.name).toLowerCase().trim().replace(/^\/+/,"");a+=`  const triggerNode_${y} = item?.json?.__triggerNode;
`,a+=`  const triggerPath_${y} = item?.json?.__triggerPath;
`,a+=`  if (!triggerNode_${y} || triggerNode_${y} === ${JSON.stringify(m.name)} || triggerPath_${y} === ${JSON.stringify(v)}) {
`,a+=`    ${w} = inputItems;
`,a+=`    ctx.setNodeOutput(${JSON.stringify(m.name)}, inputItems);
`,a+=`  }
`}),d.forEach((m)=>{let y=p.get(m.name);c+=`  let ${y}: any = inputItems;
`;let w="true",v=m.incomingConnections??[],P=v.length>0&&!m.type.includes("merge");if(v.length>0)w=`(${v.map((b)=>`ctx.isBranchActive(${JSON.stringify(b.sourceNode)}, ${b.outputIndex})`).join(" || ")})`;let E=_.mapNode({node:m,workflowName:e.name,variableName:y,previousVariableName:P?"incomingItems":"inputItems"});if(P){let S=v.map((b)=>`ctx.getNodeOutputBatches(${JSON.stringify(b.sourceNode)}, ${b.outputIndex})`);E=`await ctx.runNodeBatches(${JSON.stringify(m.name)}, [${S.join(", ")}].flat(), async (incomingItems) => {
${E.trim()}
});`}let J=`  if (${w}) {
    ${E.trim().replace(/\n/g,`
    `)}
  }
`;if(m.ancestorTriggers&&m.ancestorTriggers.length>0){let S=m.ancestorTriggers.map((b)=>`ctx.hasExecutedNode(${JSON.stringify(b)})`).join(" || ");a+=`  if (${S}) {
  ${J.trim().replace(/\n/g,`
  `)}
  }
`}else a+=J;g=m.name});let f=e.orderedNodes[e.orderedNodes.length-1]?.name,N=f?`(ctx.getNode(${JSON.stringify(f)})?.all() || [{ json: item.json }])`:"inputItems",h=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${o}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${c}
${a}
  const finalItems = ${N};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(h)}, ${t});
`}}class A{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((s)=>{let o=`workflow_${I.sanitizeIdentifier(s.fileName)}`,c=s.relativePath.replace(/\.ts$/,"");t+=`import { ${o} } from '../workflows/${c}';
`,r.push(o)});let n="",i=new Set;return e.forEach((s)=>{let o=`workflow_${I.sanitizeIdentifier(s.fileName)}`;s.nodes.filter((a)=>a.type.includes("webhook")).forEach((a)=>{let g=(a.parameters?.path||s.fileName).toLowerCase().trim().replace(/^\/+/,""),p=(a.parameters?.httpMethod||"POST").toUpperCase(),l=p==="ALL"?["GET","POST","PUT","DELETE"]:[p],d=Array.from(new Set([`/${g}`,`/webhook/${g}`]));l.forEach((f)=>{d.forEach((N)=>{let h=`${f}:${N}`;if(i.has(h))return;i.add(h),n+=`
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
          __triggerNode: ${JSON.stringify(a.name)},
          __triggerPath: ${JSON.stringify(g)}
        }
      }];

      try {
        const { items, context } = await ${o}(inputItems, ctx);
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function B(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=j.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=I.generateWorkflowCode(n),s=A.generateRoutesCode([n]),o=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:B};})();
