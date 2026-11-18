(()=>{var{defineProperty:k,getOwnPropertyNames:B,getOwnPropertyDescriptor:F}=Object,D=Object.prototype.hasOwnProperty;function z(e){return this[e]}var U=(e)=>{var t=(T??=new WeakMap).get(e),r;if(t)return t;if(t=k({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of B(e))if(!D.call(t,n))k(t,n,{get:z.bind(e,n),enumerable:!(r=F(e,n))||r.enumerable})}return T.set(e,t),t},T;var G=(e)=>e;function H(e,t){this[e]=G.bind(null,t)}var K=(e,t)=>{for(var r in t)k(e,r,{get:t[r],enumerable:!0,configurable:!0,set:H.bind(t,r)})};var Z={};K(Z,{compileWorkflowJson:()=>L});class j{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,i=e.name||t,o=e.nodes||[],s=e.connections||{},p=r||`${t}.json`,a=new Map;o.forEach((u)=>a.set(u.name,[])),Object.entries(s).forEach(([u,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((y,h)=>{if(Array.isArray(y))y.forEach((N)=>{if(N&&N.node&&a.has(N.node))a.get(N.node)?.push({sourceNode:u,outputIndex:h,inputIndex:N.index??0})})})}),o.forEach((u)=>{u.incomingConnections=a.get(u.name)||[]});let f=o.filter((u)=>u.type.includes("webhook")||u.type.includes("trigger")||u.type.includes("cron")),m=new Set(f.map((u)=>u.name));o.forEach((u)=>{let l=new Set,y=new Set,h=[...(a.get(u.name)||[]).map((N)=>N.sourceNode)];while(h.length>0){let N=h.shift();if(y.has(N))continue;if(y.add(N),m.has(N))l.add(N);(a.get(N)||[]).forEach((C)=>h.push(C.sourceNode))}u.ancestorTriggers=Array.from(l)});let d=this.topologicalSort(o,s);return{id:n,name:i,fileName:t,relativePath:p.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:d,triggers:f}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((m)=>{r.set(m.name,m),n.set(m.name,[])}),Object.entries(t).forEach(([m,d])=>{if(d.main&&Array.isArray(d.main))d.main.forEach((u)=>{if(Array.isArray(u))u.forEach((l)=>{if(l&&l.node&&r.has(l.node))n.get(m)?.push(l.node)})})});let i=new Set,o=new Set,s=[],p=(m,d=[])=>{if(i.has(m))return;if(o.has(m)){let y=[...d,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(m),d.push(m);let u=n.get(m)||[];for(let y of u)p(y,d);d.pop(),o.delete(m),i.add(m);let l=r.get(m);if(l)s.push(l)},a=new Map;e.forEach((m)=>a.set(m.name,0));for(let m of n.values())for(let d of m)a.set(d,(a.get(d)||0)+1);let f=e.map((m)=>m.name).filter((m)=>a.get(m)===0);for(let m of f)p(m);for(let m of e)if(!i.has(m.name))p(m.name);return s.reverse()}}class g{static parse(e){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let t=e.trim();if(!t.includes("{{"))return JSON.stringify(e);if(t.startsWith("=")){let s=t.substring(1).trim();if(s.startsWith("{{")&&s.endsWith("}}")){let p=s.substring(2,s.length-2).trim();if(!p.includes("{{")&&!p.includes("}}"))return`(${this.transformTokens(p)})`}t=t.substring(1).trim()}let r=[],n=0,o=t.replace(/\{\{(.*?)\}\}/g,(s,p)=>{let a=this.transformTokens(p.trim()),f=`__EXPR_PLACEHOLDER_${n++}__`;return r.push(`\${(${a}) ?? ''}`),f}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return r.forEach((s,p)=>{o=o.replace(`__EXPR_PLACEHOLDER_${p}__`,s)}),`\`${o}\``}static transformTokens(e){let t=e;return t=t.replace(/\$now\b/g,"new Date().toISOString()"),t=t.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),t=t.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(r,n,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(n)})?.item?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(r,n,i)=>{let o=i?i.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(n)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),t=t.replace(/\$\(['"](.*?)['"]\)/g,(r,n)=>`ctx.getNode(${JSON.stringify(n)})`),t=t.replace(/\$node\(['"](.*?)['"]\)/g,(r,n)=>`ctx.getNode(${JSON.stringify(n)})`),t=t.replace(/\$json([a-zA-Z0-9_.]*)/g,(r,n)=>{if(!n)return"item?.json";return`item?.json?.${n.split(".").filter(Boolean).join("?.")}`}),t=t.replace(/\$input\.first\(\)\.json/g,"item?.json"),t=t.replace(/\$input\.item\.json/g,"item?.json"),t=t.replace(/\$input\.all\(\)/g,"inputItems"),t=t.replace(/\$env\.([a-zA-Z0-9_]+)/g,(r,n)=>`process.env.${n}`),t}}function O(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function X(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:r=0}of e.incomingConnections??[])if(!Number.isInteger(r)||r<0||r>=t)return"position Merge has an invalid input port"}function W(e){let t=X(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=r.numberInputs??2,i=Array.from({length:n},()=>[]);for(let{sourceNode:a,outputIndex:f,inputIndex:m=0}of e.node.incomingConnections??[])i[m].push(`ctx.getNodeOutput(${JSON.stringify(a)}, ${f}) || []`);let o=i.map((a)=>`[${a.join(", ")}].flat()`),s=r.options??{},p=g.parse(s.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${o.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${p}), ${JSON.stringify(s.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function P(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function x(e){if(P(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function q(e,t){let r=new Map,n=[e],i=new Set([e.name]);for(let o=0;o<n.length;o++){let s=[...n[o].incomingConnections??[]];s.sort((p,a)=>(p.inputIndex??0)-(a.inputIndex??0));for(let{sourceNode:p,outputIndex:a}of s){if(i.has(p))continue;i.add(p),r.set(p,a);let f=t.get(p);if(f)n.push(f)}}return[...r]}class E{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return O(e.node)?W(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",o="item?.json || {}";if(r==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)o=g.parse(t.responseBody);else if(r==="text"&&t.responseBody)o=g.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=x(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters||{},n=r.jsCode||r.functionCode||"return $input.all();",i=e.previousVariableName||"inputItems",o=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
`:"",s=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(p,a)=>`(ctx.getNode(${JSON.stringify(a)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(p,a)=>`(ctx.getNode(${JSON.stringify(a)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(p,a)=>`(ctx.getNode(${JSON.stringify(a)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${i} || inputItems;
${o}      const result = await (async () => {
        ${s}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},r=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(r)}, ${n} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=g.parse(o.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let p of s)if(p.name){let a=g.parse(p.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${a});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[];if(Array.isArray(e?.conditions))for(let i of e.conditions){let o=g.parse(i.leftValue),s=g.parse(i.rightValue),p=i.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(p)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&i.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,s=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${s}))`;switch(p){case"equals":case"equal":r.push(`(${o} == ${s})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${s})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${s} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${s}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${s}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${s}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${s}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let n=e?.combinator==="or"?" || ":" && ";return r.length?r.join(n):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.rules?.values||[],i=t.options??{},o=i.fallbackOutput??"none",s=o==="extra"?n.length:o,a=n.length>0&&Number.isInteger(s)&&s>=0&&(o==="extra"||s<n.length)?`
        if (!matched) {
          send(item, ${s});
          matched = true;
        }`:"",m=`
      const predicates = [${n.map((u)=>`(item: N8nItem) => (${u.conditions?this.parseConditions(u.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${g.parse(i.allMatchingOutputs??!1)})) break;
          }
        }
        ${a}
        if (matched) routed.push(item);
      }`,d=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${g.parse(t.numberOutputs??4)}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${g.parse(t.output??0)};
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
      ${t.mode==="expression"?d:m}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),i=g.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${g.parse(a.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)s=`JSON.stringify(${g.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${g.parse(a.value)}`).join(", ")} })`}return`
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=g.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=g.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class I{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",i=e.nodes.some(O)?`import { mergeByPosition } from '${n}/position-merge';
`:"",o=new Set(e.nodes.filter(P)),s=o.size>0?`import { createLegacyNodeReferences } from '${n}/code-context';
`:"",p=o.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((c)=>c.name))});
`:"",a=new Map(e.nodes.map((c)=>[c.name,c])),f=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${i}${s}
${p}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,m=`  let item: N8nItem = inputItems[0] || { json: {} };
`,d=`  ctx.setNodeOutput("__initial__", inputItems);
`,u="inputItems",l=new Map;e.orderedNodes.forEach((c,b)=>{let w=`node_${b}_${this.sanitizeIdentifier(c.name)}`;l.set(c.name,w)});let y=e.orderedNodes.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),h=e.orderedNodes.filter((c)=>!y.includes(c));y.forEach((c)=>{let b=e.orderedNodes.indexOf(c),w=l.get(c.name);m+=`  let ${w}: any = inputItems;
`;let S=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");d+=`  const triggerNode_${b} = item?.json?.__triggerNode;
`,d+=`  const triggerPath_${b} = item?.json?.__triggerPath;
`,d+=`  if (!triggerNode_${b} || triggerNode_${b} === ${JSON.stringify(c.name)} || triggerPath_${b} === ${JSON.stringify(S)}) {
`,d+=`    ${w} = inputItems;
`,d+=`    ctx.setNodeOutput(${JSON.stringify(c.name)}, inputItems);
`,d+=`  }
`}),h.forEach((c)=>{let b=l.get(c.name);m+=`  let ${b}: any = inputItems;
`;let w="inputItems",S="true";if(c.incomingConnections&&c.incomingConnections.length>0){if(c.incomingConnections.length===1){let v=c.incomingConnections[0];w=`ctx.getNodeOutput(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`}else w=`(${c.incomingConnections.map((R)=>`ctx.getNodeOutput(${JSON.stringify(R.sourceNode)}, ${R.outputIndex})`).join(" || ")})`;S=`(${c.incomingConnections.map((v)=>`ctx.isBranchActive(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`).join(" || ")})`}let V=E.mapNode({node:c,workflowName:e.name,variableName:b,previousVariableName:w,legacyNodePorts:o.has(c)?q(c,a):void 0}),A=`  if (${S}) {
    ${V.trim().replace(/\n/g,`
    `)}
  }
`;if(c.ancestorTriggers&&c.ancestorTriggers.length>0){let M=c.ancestorTriggers.map((v)=>`ctx.hasExecutedNode(${JSON.stringify(v)})`).join(" || ");d+=`  if (${M}) {
  ${A.trim().replace(/\n/g,`
  `)}
  }
`}else d+=A;u=c.name});let N=e.orderedNodes[e.orderedNodes.length-1]?.name,_=N?`(ctx.getNode(${JSON.stringify(N)})?.all() || [{ json: item.json }])`:"inputItems",C=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${f}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${m}
${d}
  const finalItems = ${_};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(C)}, ${t});
`}}class J{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((o)=>{let s=`workflow_${I.sanitizeIdentifier(o.fileName)}`,p=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${p}';
`,r.push(s)});let n="",i=new Set;return e.forEach((o)=>{let s=`workflow_${I.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((a)=>a.type.includes("webhook")).forEach((a)=>{let f=(a.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),m=(a.parameters?.httpMethod||"POST").toUpperCase(),d=m==="ALL"?["GET","POST","PUT","DELETE"]:[m],u=Array.from(new Set([`/${f}`,`/webhook/${f}`]));d.forEach((l)=>{u.forEach((y)=>{let h=`${l}:${y}`;if(i.has(h))return;i.add(h),n+=`
  fastify.route({
    method: '${l}',
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
          __triggerNode: ${JSON.stringify(a.name)},
          __triggerPath: ${JSON.stringify(f)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function L(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=j.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=I.generateWorkflowCode(n),o=J.generateRoutesCode([n]),s=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:s,workflowCode:i,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:L};})();
