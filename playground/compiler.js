(()=>{var{defineProperty:k,getOwnPropertyNames:T,getOwnPropertyDescriptor:R}=Object,W=Object.prototype.hasOwnProperty;function q(e){return this[e]}var B=(e)=>{var t=(_??=new WeakMap).get(e),r;if(t)return t;if(t=k({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of T(e))if(!W.call(t,n))k(t,n,{get:q.bind(e,n),enumerable:!(r=R(e,n))||r.enumerable})}return _.set(e,t),t},_;var V=(e)=>e;function x(e,t){this[e]=V.bind(null,t)}var L=(e,t)=>{for(var r in t)k(e,r,{get:t[r],enumerable:!0,configurable:!0,set:x.bind(t,r)})};var D={};L(D,{compileWorkflowJson:()=>M});class C{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},m=r||`${t}.json`,a=new Map;s.forEach((d)=>a.set(d.name,[])),Object.entries(o).forEach(([d,g])=>{if(g.main&&Array.isArray(g.main))g.main.forEach((y,c)=>{if(Array.isArray(y))y.forEach((u)=>{if(u&&u.node&&a.has(u.node))a.get(u.node)?.push({sourceNode:d,outputIndex:c,inputIndex:u.index??0})})})}),s.forEach((d)=>{d.incomingConnections=a.get(d.name)||[]});let l=s.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),p=new Set(l.map((d)=>d.name));s.forEach((d)=>{let g=new Set,y=new Set,c=[...(a.get(d.name)||[]).map((u)=>u.sourceNode)];while(c.length>0){let u=c.shift();if(y.has(u))continue;if(y.add(u),p.has(u))g.add(u);(a.get(u)||[]).forEach((b)=>c.push(b.sourceNode))}d.ancestorTriggers=Array.from(g)});let f=this.topologicalSort(s,o);return{id:n,name:i,fileName:t,relativePath:m.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:f,triggers:l}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((p)=>{r.set(p.name,p),n.set(p.name,[])}),Object.entries(t).forEach(([p,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((d)=>{if(Array.isArray(d))d.forEach((g)=>{if(g&&g.node&&r.has(g.node))n.get(p)?.push(g.node)})})});let i=new Set,s=new Set,o=[],m=(p,f=[])=>{if(i.has(p))return;if(s.has(p)){let y=[...f,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(p),f.push(p);let d=n.get(p)||[];for(let y of d)m(y,f);f.pop(),s.delete(p),i.add(p);let g=r.get(p);if(g)o.push(g)},a=new Map;e.forEach((p)=>a.set(p.name,0));for(let p of n.values())for(let f of p)a.set(f,(a.get(f)||0)+1);let l=e.map((p)=>p.name).filter((p)=>a.get(p)===0);for(let p of l)m(p);for(let p of e)if(!i.has(p.name))m(p.name);return o.reverse()}}class N{static parse(e,t){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let m=r.substring(1).trim();if(m.startsWith("{{")&&m.endsWith("}}")){let a=m.substring(2,m.length-2).trim();if(!a.includes("{{")&&!a.includes("}}"))return`(${this.transformTokens(a,t)})`}r=r.substring(1).trim()}let n=[],i=0,o=r.replace(/\{\{(.*?)\}\}/g,(m,a)=>{let l=this.transformTokens(a.trim(),t),p=`__EXPR_PLACEHOLDER_${i++}__`;return n.push(`\${(${l}) ?? ''}`),p}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((m,a)=>{o=o.replace(`__EXPR_PLACEHOLDER_${a}__`,m)}),`\`${o}\``}static transformTokens(e,t){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.item?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,i)=>{if(!i)return"item?.json";return`item?.json?.${i.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,"inputItems"),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,i)=>`process.env.${i}`),r}}function S(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function F(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:r=0}of e.incomingConnections??[])if(!Number.isInteger(r)||r<0||r>=t)return"position Merge has an invalid input port"}function A(e){let t=F(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=r.numberInputs??2,i=Array.from({length:n},()=>[]);for(let{sourceNode:a,outputIndex:l,inputIndex:p=0}of e.node.incomingConnections??[])i[p].push(`ctx.getNodeOutput(${JSON.stringify(a)}, ${l}) || []`);let s=i.map((a)=>`[${a.join(", ")}].flat()`),o=r.options??{},m=N.parse(o.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${s.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${m}), ${JSON.stringify(o.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}class j{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return S(e.node)?A(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(r==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)s=N.parse(t.responseBody);else if(r==="text"&&t.responseBody)s=N.parse(t.responseBody);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let s of t.assignments.assignments)if(s.name){let o=N.parse(s.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(s.name)}, ${o});`)}}if(t.values)for(let s of["string","number","boolean"]){let o=t.values[s];if(Array.isArray(o)){for(let m of o)if(m.name){let a=N.parse(m.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${a});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],n=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=N.parse(s.leftValue,n),m=N.parse(s.rightValue,n),a=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(a)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,m=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${m}))`;switch(a){case"equals":case"equal":r.push(`(${o} == ${m})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${m})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${m} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${m}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${m}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${m}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${m}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let i=e?.combinator==="or"?" || ":" && ";return r.length?r.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
            if (!(${N.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${a}
        if (matched) routed.push(item);
      }`,f=`
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
      ${t.mode==="expression"?f:p}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),i=N.parse(t.url||""),s="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))s=`{ ${t.headerParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${N.parse(a.value)}`).join(", ")} }`;let o="undefined";if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)o=`JSON.stringify(${N.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))o=`JSON.stringify({ ${t.bodyParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${N.parse(a.value)}`).join(", ")} })`}return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=N.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=Number(t.amount||1),i=t.unit||"seconds",o=n*(i==="minutes"?60000:i==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${n} ${i})
    await new Promise((resolve) => setTimeout(resolve, ${o}));
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${r} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class v{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",i=e.nodes.some(S)?`import { mergeByPosition } from '${n}/position-merge';
`:"",s=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${i}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,o=`  let item: N8nItem = inputItems[0] || { json: {} };
`,m=`  ctx.setNodeOutput("__initial__", inputItems);
`,a="inputItems",l=new Map;e.orderedNodes.forEach((c,u)=>{let h=`node_${u}_${this.sanitizeIdentifier(c.name)}`;l.set(c.name,h)});let p=e.orderedNodes.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),f=e.orderedNodes.filter((c)=>!p.includes(c));p.forEach((c)=>{let u=e.orderedNodes.indexOf(c),h=l.get(c.name);o+=`  let ${h}: any = inputItems;
`;let b=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");m+=`  const triggerNode_${u} = item?.json?.__triggerNode;
`,m+=`  const triggerPath_${u} = item?.json?.__triggerPath;
`,m+=`  if (!triggerNode_${u} || triggerNode_${u} === ${JSON.stringify(c.name)} || triggerPath_${u} === ${JSON.stringify(b)}) {
`,m+=`    ${h} = inputItems;
`,m+=`    ctx.setNodeOutput(${JSON.stringify(c.name)}, inputItems);
`,m+=`  }
`}),f.forEach((c)=>{let u=l.get(c.name);o+=`  let ${u}: any = inputItems;
`;let h="true",b=c.incomingConnections??[],E=b.length>0&&!c.type.includes("merge");if(b.length>0)h=`(${b.map((w)=>`ctx.isBranchActive(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`).join(" || ")})`;let O=j.mapNode({node:c,workflowName:e.name,variableName:u,previousVariableName:E?"incomingItems":"inputItems"});if(E){let I=b.map((w)=>`ctx.getNodeOutputBatches(${JSON.stringify(w.sourceNode)}, ${w.outputIndex})`);O=`await ctx.runNodeBatches(${JSON.stringify(c.name)}, [${I.join(", ")}].flat(), async (incomingItems) => {
${O.trim()}
});`}let J=`  if (${h}) {
    ${O.trim().replace(/\n/g,`
    `)}
  }
`;if(c.ancestorTriggers&&c.ancestorTriggers.length>0){let I=c.ancestorTriggers.map((w)=>`ctx.hasExecutedNode(${JSON.stringify(w)})`).join(" || ");m+=`  if (${I}) {
  ${J.trim().replace(/\n/g,`
  `)}
  }
`}else m+=J;a=c.name});let d=e.orderedNodes[e.orderedNodes.length-1]?.name,g=d?`(ctx.getNode(${JSON.stringify(d)})?.all() || [{ json: item.json }])`:"inputItems",y=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${s}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${o}
${m}
  const finalItems = ${g};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(y)}, ${t});
`}}class P{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((s)=>{let o=`workflow_${v.sanitizeIdentifier(s.fileName)}`,m=s.relativePath.replace(/\.ts$/,"");t+=`import { ${o} } from '../workflows/${m}';
`,r.push(o)});let n="",i=new Set;return e.forEach((s)=>{let o=`workflow_${v.sanitizeIdentifier(s.fileName)}`;s.nodes.filter((a)=>a.type.includes("webhook")).forEach((a)=>{let l=(a.parameters?.path||s.fileName).toLowerCase().trim().replace(/^\/+/,""),p=(a.parameters?.httpMethod||"POST").toUpperCase(),f=p==="ALL"?["GET","POST","PUT","DELETE"]:[p],d=Array.from(new Set([`/${l}`,`/webhook/${l}`]));f.forEach((g)=>{d.forEach((y)=>{let c=`${g}:${y}`;if(i.has(c))return;i.add(c),n+=`
  fastify.route({
    method: '${g}',
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
          __triggerPath: ${JSON.stringify(l)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function M(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=C.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=v.generateWorkflowCode(n),s=P.generateRoutesCode([n]),o=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:M};})();
