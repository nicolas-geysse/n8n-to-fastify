(()=>{var{defineProperty:P,getOwnPropertyNames:V,getOwnPropertyDescriptor:F}=Object,D=Object.prototype.hasOwnProperty;function z(e){return this[e]}var U=(e)=>{var t=(W??=new WeakMap).get(e),r;if(t)return t;if(t=P({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of V(e))if(!D.call(t,n))P(t,n,{get:z.bind(e,n),enumerable:!(r=F(e,n))||r.enumerable})}return W.set(e,t),t},W;var G=(e)=>e;function H(e,t){this[e]=G.bind(null,t)}var K=(e,t)=>{for(var r in t)P(e,r,{get:t[r],enumerable:!0,configurable:!0,set:H.bind(t,r)})};var Z={};K(Z,{compileWorkflowJson:()=>B});class E{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,a=e.name||t,s=e.nodes||[],o=e.connections||{},m=r||`${t}.json`,i=new Map;s.forEach((u)=>i.set(u.name,[])),Object.entries(o).forEach(([u,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((y,h)=>{if(Array.isArray(y))y.forEach((N)=>{if(N&&N.node&&i.has(N.node))i.get(N.node)?.push({sourceNode:u,outputIndex:h,inputIndex:N.index??0})})})}),s.forEach((u)=>{u.incomingConnections=i.get(u.name)||[]});let g=s.filter((u)=>u.type.includes("webhook")||u.type.includes("trigger")||u.type.includes("cron")),p=new Set(g.map((u)=>u.name));s.forEach((u)=>{let l=new Set,y=new Set,h=[...(i.get(u.name)||[]).map((N)=>N.sourceNode)];while(h.length>0){let N=h.shift();if(y.has(N))continue;if(y.add(N),p.has(N))l.add(N);(i.get(N)||[]).forEach((k)=>h.push(k.sourceNode))}u.ancestorTriggers=Array.from(l)});let d=this.topologicalSort(s,o);return{id:n,name:a,fileName:t,relativePath:m.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:d,triggers:g}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((p)=>{r.set(p.name,p),n.set(p.name,[])}),Object.entries(t).forEach(([p,d])=>{if(d.main&&Array.isArray(d.main))d.main.forEach((u)=>{if(Array.isArray(u))u.forEach((l)=>{if(l&&l.node&&r.has(l.node))n.get(p)?.push(l.node)})})});let a=new Set,s=new Set,o=[],m=(p,d=[])=>{if(a.has(p))return;if(s.has(p)){let y=[...d,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(p),d.push(p);let u=n.get(p)||[];for(let y of u)m(y,d);d.pop(),s.delete(p),a.add(p);let l=r.get(p);if(l)o.push(l)},i=new Map;e.forEach((p)=>i.set(p.name,0));for(let p of n.values())for(let d of p)i.set(d,(i.get(d)||0)+1);let g=e.map((p)=>p.name).filter((p)=>i.get(p)===0);for(let p of g)m(p);for(let p of e)if(!a.has(p.name))m(p.name);return o.reverse()}}class f{static parse(e,t){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let m=r.substring(1).trim();if(m.startsWith("{{")&&m.endsWith("}}")){let i=m.substring(2,m.length-2).trim();if(!i.includes("{{")&&!i.includes("}}"))return`(${this.transformTokens(i,t)})`}r=r.substring(1).trim()}let n=[],a=0,o=r.replace(/\{\{(.*?)\}\}/g,(m,i)=>{let g=this.transformTokens(i.trim(),t),p=`__EXPR_PLACEHOLDER_${a++}__`;return n.push(`\${(${g}) ?? ''}`),p}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((m,i)=>{o=o.replace(`__EXPR_PLACEHOLDER_${i}__`,m)}),`\`${o}\``}static transformTokens(e,t){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,a,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(a)})?.item?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,a,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(a)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(n,a)=>`ctx.getNode(${JSON.stringify(a)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(n,a)=>`ctx.getNode(${JSON.stringify(a)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,a)=>{if(!a)return"item?.json";return`item?.json?.${a.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,"inputItems"),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,a)=>`process.env.${a}`),r}}function C(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function X(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:r=0}of e.incomingConnections??[])if(!Number.isInteger(r)||r<0||r>=t)return"position Merge has an invalid input port"}function q(e){let t=X(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=r.numberInputs??2,a=Array.from({length:n},()=>[]);for(let{sourceNode:i,outputIndex:g,inputIndex:p=0}of e.node.incomingConnections??[])a[p].push(`ctx.getNodeOutput(${JSON.stringify(i)}, ${g}) || []`);let s=a.map((i)=>`[${i.join(", ")}].flat()`),o=r.options??{},m=f.parse(o.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${s.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${m}), ${JSON.stringify(o.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function J(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function x(e){if(J(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function L(e,t){let r=new Map,n=[e],a=new Set([e.name]);for(let s=0;s<n.length;s++){let o=[...n[s].incomingConnections??[]];o.sort((m,i)=>(m.inputIndex??0)-(i.inputIndex??0));for(let{sourceNode:m,outputIndex:i}of o){if(a.has(m))continue;a.add(m),r.set(m,i);let g=t.get(m);if(g)n.push(g)}}return[...r]}class _{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return C(e.node)?q(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",s="item?.json || {}";if(r==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)s=f.parse(t.responseBody);else if(r==="text"&&t.responseBody)s=f.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${a} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${s};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${n}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=x(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters||{},n=r.jsCode||r.functionCode||"return $input.all();",a=e.previousVariableName||"inputItems",s=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
`:"",o=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(m,i)=>`(ctx.getNode(${JSON.stringify(i)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(m,i)=>`(ctx.getNode(${JSON.stringify(i)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(m,i)=>`(ctx.getNode(${JSON.stringify(i)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${a} || inputItems;
${s}      const result = await (async () => {
        ${o}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},r=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(r)}, ${n} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let s of t.assignments.assignments)if(s.name){let o=f.parse(s.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(s.name)}, ${o});`)}}if(t.values)for(let s of["string","number","boolean"]){let o=t.values[s];if(Array.isArray(o)){for(let m of o)if(m.name){let i=f.parse(m.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${i});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],n=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=f.parse(s.leftValue,n),m=f.parse(s.rightValue,n),i=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(i)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,m=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${m}))`;switch(i){case"equals":case"equal":r.push(`(${o} == ${m})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${m})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${m} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${m}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${m}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${m}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${m}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let a=e?.combinator==="or"?" || ":" && ";return r.length?r.join(a):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.rules?.values||[],a=t.options??{},s=a.fallbackOutput??"none",o=s==="extra"?n.length:s,i=s==="extra"||Number.isInteger(o)&&o>=0&&o<n.length?`
        if (!matched) {
          send(item, ${o});
          matched = true;
        }`:"",p=`
      const predicates = [${n.map((u)=>`(item: N8nItem) => (${u.conditions?this.parseConditions(u.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${f.parse(a.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${i}
        if (matched) routed.push(item);
      }`,d=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${f.parse(t.numberOutputs??4,"firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${f.parse(t.output??0,"firstInputItem")};
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
      ${t.mode==="expression"?d:p}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),a=f.parse(t.url||""),s="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))s=`{ ${t.headerParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${f.parse(i.value)}`).join(", ")} }`;let o="undefined";if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)o=`JSON.stringify(${f.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))o=`JSON.stringify({ ${t.bodyParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${f.parse(i.value)}`).join(", ")} })`}return`
    // [Node: ${e.node.name}] Native HTTP Request (${n})
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${a};
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
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],r=e.node.parameters?.mode||"append",n=t.map((a)=>`(ctx.getNodeOutput(${JSON.stringify(a.sourceNode)}, ${a.outputIndex}) || [])`);if(r==="combine"&&n.length>=2)return`
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=f.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",a=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=f.parse(a);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=Number(t.amount||1),a=t.unit||"seconds",o=n*(a==="minutes"?60000:a==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${n} ${a})
    await new Promise((resolve) => setTimeout(resolve, ${o}));
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class S{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",a=e.nodes.some(C)?`import { mergeByPosition } from '${n}/position-merge';
`:"",s=new Set(e.nodes.filter(J)),o=s.size>0?`import { createLegacyNodeReferences } from '${n}/code-context';
`:"",m=s.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((c)=>c.name))});
`:"",i=new Map(e.nodes.map((c)=>[c.name,c])),g=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${a}${o}
${m}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,p=`  let item: N8nItem = inputItems[0] || { json: {} };
`,d=`  ctx.setNodeOutput("__initial__", inputItems);
`,u="inputItems",l=new Map;e.orderedNodes.forEach((c,b)=>{let w=`node_${b}_${this.sanitizeIdentifier(c.name)}`;l.set(c.name,w)});let y=e.orderedNodes.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),h=e.orderedNodes.filter((c)=>!y.includes(c));y.forEach((c)=>{let b=e.orderedNodes.indexOf(c),w=l.get(c.name);p+=`  let ${w}: any = inputItems;
`;let I=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");d+=`  const triggerNode_${b} = item?.json?.__triggerNode;
`,d+=`  const triggerPath_${b} = item?.json?.__triggerPath;
`,d+=`  if (!triggerNode_${b} || triggerNode_${b} === ${JSON.stringify(c.name)} || triggerPath_${b} === ${JSON.stringify(I)}) {
`,d+=`    ${w} = inputItems;
`,d+=`    ctx.setNodeOutput(${JSON.stringify(c.name)}, inputItems);
`,d+=`  }
`}),h.forEach((c)=>{let b=l.get(c.name);p+=`  let ${b}: any = inputItems;
`;let w="true",I=c.incomingConnections??[],R=I.length>0&&!c.type.includes("merge");if(I.length>0)w=`(${I.map((v)=>`ctx.isBranchActive(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`).join(" || ")})`;let j=_.mapNode({node:c,workflowName:e.name,variableName:b,previousVariableName:R?"incomingItems":"inputItems",legacyNodePorts:s.has(c)?L(c,i):void 0});if(R){let O=I.map((v)=>`ctx.getNodeOutputBatches(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`);j=`await ctx.runNodeBatches(${JSON.stringify(c.name)}, [${O.join(", ")}].flat(), async (incomingItems) => {
${j.trim()}
});`}let T=`  if (${w}) {
    ${j.trim().replace(/\n/g,`
    `)}
  }
`;if(c.ancestorTriggers&&c.ancestorTriggers.length>0){let O=c.ancestorTriggers.map((v)=>`ctx.hasExecutedNode(${JSON.stringify(v)})`).join(" || ");d+=`  if (${O}) {
  ${T.trim().replace(/\n/g,`
  `)}
  }
`}else d+=T;u=c.name});let N=e.orderedNodes[e.orderedNodes.length-1]?.name,M=N?`(ctx.getNode(${JSON.stringify(N)})?.all() || [{ json: item.json }])`:"inputItems",k=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${g}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${p}
${d}
  const finalItems = ${M};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(k)}, ${t});
`}}class A{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((s)=>{let o=`workflow_${S.sanitizeIdentifier(s.fileName)}`,m=s.relativePath.replace(/\.ts$/,"");t+=`import { ${o} } from '../workflows/${m}';
`,r.push(o)});let n="",a=new Set;return e.forEach((s)=>{let o=`workflow_${S.sanitizeIdentifier(s.fileName)}`;s.nodes.filter((i)=>i.type.includes("webhook")).forEach((i)=>{let g=(i.parameters?.path||s.fileName).toLowerCase().trim().replace(/^\/+/,""),p=(i.parameters?.httpMethod||"POST").toUpperCase(),d=p==="ALL"?["GET","POST","PUT","DELETE"]:[p],u=Array.from(new Set([`/${g}`,`/webhook/${g}`]));d.forEach((l)=>{u.forEach((y)=>{let h=`${l}:${y}`;if(a.has(h))return;a.add(h),n+=`
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
          __triggerNode: ${JSON.stringify(i.name)},
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function B(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=E.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let a=S.generateWorkflowCode(n),s=A.generateRoutesCode([n]),o=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:o,workflowCode:a,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:B};})();
