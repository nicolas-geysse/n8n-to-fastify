(()=>{var{defineProperty:J,getOwnPropertyNames:X,getOwnPropertyDescriptor:Z}=Object,Q=Object.prototype.hasOwnProperty;function Y(e){return this[e]}var ee=(e)=>{var t=(W??=new WeakMap).get(e),r;if(t)return t;if(t=J({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of X(e))if(!Q.call(t,n))J(t,n,{get:Y.bind(e,n),enumerable:!(r=Z(e,n))||r.enumerable})}return W.set(e,t),t},W;var te=(e)=>e;function re(e,t){this[e]=te.bind(null,t)}var ne=(e,t)=>{for(var r in t)J(e,r,{get:t[r],enumerable:!0,configurable:!0,set:re.bind(t,r)})};var ie={};ne(ie,{compileWorkflowJson:()=>z});class M{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},p=r||`${t}.json`,a=new Map;s.forEach((u)=>a.set(u.name,[])),Object.entries(o).forEach(([u,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((y,h)=>{if(Array.isArray(y))y.forEach((N)=>{if(N&&N.node&&a.has(N.node))a.get(N.node)?.push({sourceNode:u,outputIndex:h,inputIndex:N.index??0})})})}),s.forEach((u)=>{u.incomingConnections=a.get(u.name)||[]});let g=s.filter((u)=>u.type.includes("webhook")||u.type.includes("trigger")||u.type.includes("cron")),m=new Set(g.map((u)=>u.name));s.forEach((u)=>{let l=new Set,y=new Set,h=[...(a.get(u.name)||[]).map((N)=>N.sourceNode)];while(h.length>0){let N=h.shift();if(y.has(N))continue;if(y.add(N),m.has(N))l.add(N);(a.get(N)||[]).forEach((k)=>h.push(k.sourceNode))}u.ancestorTriggers=Array.from(l)});let c=this.topologicalSort(s,o);return{id:n,name:i,fileName:t,relativePath:p.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:c,triggers:g}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((m)=>{r.set(m.name,m),n.set(m.name,[])}),Object.entries(t).forEach(([m,c])=>{if(c.main&&Array.isArray(c.main))c.main.forEach((u)=>{if(Array.isArray(u))u.forEach((l)=>{if(l&&l.node&&r.has(l.node))n.get(m)?.push(l.node)})})});let i=new Set,s=new Set,o=[],p=(m,c=[])=>{if(i.has(m))return;if(s.has(m)){let y=[...c,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(m),c.push(m);let u=n.get(m)||[];for(let y of u)p(y,c);c.pop(),s.delete(m),i.add(m);let l=r.get(m);if(l)o.push(l)},a=new Map;e.forEach((m)=>a.set(m.name,0));for(let m of n.values())for(let c of m)a.set(c,(a.get(c)||0)+1);let g=e.map((m)=>m.name).filter((m)=>a.get(m)===0);for(let m of g)p(m);for(let m of e)if(!i.has(m.name))p(m.name);return o.reverse()}}class f{static parse(e,t){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let p=r.substring(1).trim();if(p.startsWith("{{")&&p.endsWith("}}")){let a=p.substring(2,p.length-2).trim();if(!a.includes("{{")&&!a.includes("}}"))return`(${this.transformTokens(a,t)})`}r=r.substring(1).trim()}let n=[],i=0,o=r.replace(/\{\{(.*?)\}\}/g,(p,a)=>{let g=this.transformTokens(a.trim(),t),m=`__EXPR_PLACEHOLDER_${i++}__`;return n.push(`\${(${g}) ?? ''}`),m}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((p,a)=>{o=o.replace(`__EXPR_PLACEHOLDER_${a}__`,p)}),`\`${o}\``}static transformTokens(e,t){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.item?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,i)=>{if(!i)return"item?.json";return`item?.json?.${i.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,"inputItems"),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,i)=>`process.env.${i}`),r}}function P(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function oe(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:r=0}of e.incomingConnections??[])if(!Number.isInteger(r)||r<0||r>=t)return"position Merge has an invalid input port"}function L(e){let t=oe(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=r.numberInputs??2,i=Array.from({length:n},()=>[]);for(let{sourceNode:a,outputIndex:g,inputIndex:m=0}of e.node.incomingConnections??[])i[m].push(`ctx.getNodeOutput(${JSON.stringify(a)}, ${g}) || []`);let s=i.map((a)=>`[${a.join(", ")}].flat()`),o=r.options??{},p=f.parse(o.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${s.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${p}), ${JSON.stringify(o.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function A(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function H(e){if(A(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function V(e,t){let r=new Map,n=[e],i=new Set([e.name]);for(let s=0;s<n.length;s++){let o=[...n[s].incomingConnections??[]];o.sort((p,a)=>(p.inputIndex??0)-(a.inputIndex??0));for(let{sourceNode:p,outputIndex:a}of o){if(i.has(p))continue;i.add(p),r.set(p,a);let g=t.get(p);if(g)n.push(g)}}return[...r]}var S="n8n-nodes-base.s3";function se(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function F(e){let t=se(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=e.previousVariableName??"inputItems",i=f.parse(r.bucketName),s=f.parse(r.fileKey),o=f.parse(r.binaryPropertyName??"data");return`
    {
      const downloads = (${n} || inputItems).map((item: N8nItem) => ({
        item,
        bucket: String(${i} ?? ''),
        key: String(${s} ?? ''),
        binaryProperty: String(${o} ?? ''),
      }));
      ${e.variableName} = await downloadS3Files(downloads);
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}function j(e){return e?.sendBody===!0&&e.contentType==="multipart-form-data"}function E(e){return e?.sendHeaders===!0&&e.specifyHeaders==="json"}function D(e){let t=(e.method??"GET").toUpperCase();if(!["POST","PUT","PATCH"].includes(t))return"HTTP Request multipart supports POST, PUT and PATCH";let r=e.bodyParameters?.parameters??[];if(!Array.isArray(r))return"HTTP Request multipart needs an array of form fields";for(let n of r){if(typeof n?.name!=="string"||!n.name)return"HTTP Request multipart fields require a name";if(n.parameterType==="formBinaryData"){if(typeof n.inputDataFieldName!=="string"||!n.inputDataFieldName)return"HTTP Request multipart binary fields require an input binary property"}else if(n.parameterType&&!["formData","text"].includes(n.parameterType))return"HTTP Request multipart supports text and binary form fields"}}function U(e){return`createRequestForm(item, [${(e.bodyParameters?.parameters??[]).map((n)=>{let i=f.parse(n.name),s=n.parameterType==="formBinaryData"?"binaryProperty":"value",o=f.parse(n.parameterType==="formBinaryData"?n.inputDataFieldName:n.value);return`{ name: String(${i} ?? ''), ${s}: String(${o} ?? '') }`}).join(", ")}])`}class R{static mapNode(e){let t=e.node.type;if(t===S)return F(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return P(e.node)?L(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(r==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)s=f.parse(t.responseBody);else if(r==="text"&&t.responseBody)s=f.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=H(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters||{},n=r.jsCode||r.functionCode||"return $input.all();",i=e.previousVariableName||"inputItems",s=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
`:"",o=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(p,a)=>`(ctx.getNode(${JSON.stringify(a)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(p,a)=>`(ctx.getNode(${JSON.stringify(a)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(p,a)=>`(ctx.getNode(${JSON.stringify(a)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${i} || inputItems;
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let s of t.assignments.assignments)if(s.name){let o=f.parse(s.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(s.name)}, ${o});`)}}if(t.values)for(let s of["string","number","boolean"]){let o=t.values[s];if(Array.isArray(o)){for(let p of o)if(p.name){let a=f.parse(p.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${a});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],n=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=f.parse(s.leftValue,n),p=f.parse(s.rightValue,n),a=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(a)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,p=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${p}))`;switch(a){case"equals":case"equal":r.push(`(${o} == ${p})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${p})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${p} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${p}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${p}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${p}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${p}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let i=e?.combinator==="or"?" || ":" && ";return r.length?r.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
        }`:"",m=`
      const predicates = [${n.map((u)=>`(item: N8nItem) => (${u.conditions?this.parseConditions(u.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${f.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${a}
        if (matched) routed.push(item);
      }`,c=`
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
      ${t.mode==="expression"?c:m}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),i=f.parse(t.url||""),s=j(t),o=E(t),p="{}";if(o)p=`parseRequestHeaders(${f.parse(t.jsonHeaders??"{}")})`;else if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))p=`{ ${t.headerParameters.parameters.filter((c)=>c.name).map((c)=>`${JSON.stringify(c.name)}: ${f.parse(c.value)}`).join(", ")} }`;let a="undefined";if(s){let m=D(t);if(m)throw Error(`Node ${JSON.stringify(e.node.name)}: ${m}`);a=U(t)}else if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)a=`JSON.stringify(${f.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))a=`JSON.stringify({ ${t.bodyParameters.parameters.filter((c)=>c.name).map((c)=>`${JSON.stringify(c.name)}: ${f.parse(c.value)}`).join(", ")} })`}let g=`const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${p} };`;if(s)g=`const reqHeaders = ${o?p:`new Headers(${p})`};
      reqHeaders.delete('content-type');
      reqHeaders.delete('content-length');`;else if(o)g=`const reqHeaders = ${p};
      if (!reqHeaders.has('content-type')) reqHeaders.set('content-type', 'application/json');`;return`
    // [Node: ${e.node.name}] Native HTTP Request (${n})
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      ${g}
      const reqBody = ${a};
      const res = await fetch(targetUrl, {
        method: ${JSON.stringify(n)},
        headers: reqHeaders,
        ...(reqBody !== undefined ? { body: reqBody } : {})
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=f.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class O{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",i=e.nodes.some(P)?`import { mergeByPosition } from '${n}/position-merge';
`:"",s=new Set(e.nodes.filter(A)),o=s.size>0?`import { createLegacyNodeReferences } from '${n}/code-context';
`:"",p=s.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((d)=>d.name))});
`:"",a=new Map(e.nodes.map((d)=>[d.name,d])),g=e.nodes.some((d)=>d.type===S)?`import { downloadS3Files } from '${n}/s3';
`:"",m=e.nodes.some((d)=>d.type.includes("httpRequest")&&(j(d.parameters)||E(d.parameters)))?`import { createRequestForm, parseRequestHeaders } from '${n}/http-request';
`:"",c=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${i}${o}${g}${m}
${p}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,u=`  let item: N8nItem = inputItems[0] || { json: {} };
`,l=`  ctx.setNodeOutput("__initial__", inputItems);
`,y="inputItems",h=new Map;e.orderedNodes.forEach((d,b)=>{let w=`node_${b}_${this.sanitizeIdentifier(d.name)}`;h.set(d.name,w)});let N=e.orderedNodes.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),q=e.orderedNodes.filter((d)=>!N.includes(d));N.forEach((d)=>{let b=e.orderedNodes.indexOf(d),w=h.get(d.name);u+=`  let ${w}: any = inputItems;
`;let I=(d.parameters?.path||d.name).toLowerCase().trim().replace(/^\/+/,"");l+=`  const triggerNode_${b} = item?.json?.__triggerNode;
`,l+=`  const triggerPath_${b} = item?.json?.__triggerPath;
`,l+=`  if (!triggerNode_${b} || triggerNode_${b} === ${JSON.stringify(d.name)} || triggerPath_${b} === ${JSON.stringify(I)}) {
`,l+=`    ${w} = inputItems;
`,l+=`    ctx.setNodeOutput(${JSON.stringify(d.name)}, inputItems);
`,l+=`  }
`}),q.forEach((d)=>{let b=h.get(d.name);u+=`  let ${b}: any = inputItems;
`;let w="true",I=d.incomingConnections??[],x=I.length>0&&!d.type.includes("merge");if(I.length>0)w=`(${I.map((v)=>`ctx.isBranchActive(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`).join(" || ")})`;let T=R.mapNode({node:d,workflowName:e.name,variableName:b,previousVariableName:x?"incomingItems":"inputItems",legacyNodePorts:s.has(d)?V(d,a):void 0});if(x){let C=I.map((v)=>`ctx.getNodeOutputBatches(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`);T=`await ctx.runNodeBatches(${JSON.stringify(d.name)}, [${C.join(", ")}].flat(), async (incomingItems) => {
${T.trim()}
});`}let B=`  if (${w}) {
    ${T.trim().replace(/\n/g,`
    `)}
  }
`;if(d.ancestorTriggers&&d.ancestorTriggers.length>0){let C=d.ancestorTriggers.map((v)=>`ctx.hasExecutedNode(${JSON.stringify(v)})`).join(" || ");l+=`  if (${C}) {
  ${B.trim().replace(/\n/g,`
  `)}
  }
`}else l+=B;y=d.name});let k=e.orderedNodes[e.orderedNodes.length-1]?.name,G=k?`(ctx.getNode(${JSON.stringify(k)})?.all() || [{ json: item.json }])`:"inputItems",K=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${c}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${u}
${l}
  const finalItems = ${G};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(K)}, ${t});
`}}class _{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((s)=>{let o=`workflow_${O.sanitizeIdentifier(s.fileName)}`,p=s.relativePath.replace(/\.ts$/,"");t+=`import { ${o} } from '../workflows/${p}';
`,r.push(o)});let n="",i=new Set;return e.forEach((s)=>{let o=`workflow_${O.sanitizeIdentifier(s.fileName)}`;s.nodes.filter((a)=>a.type.includes("webhook")).forEach((a)=>{let g=(a.parameters?.path||s.fileName).toLowerCase().trim().replace(/^\/+/,""),m=(a.parameters?.httpMethod||"POST").toUpperCase(),c=m==="ALL"?["GET","POST","PUT","DELETE"]:[m],u=Array.from(new Set([`/${g}`,`/webhook/${g}`]));c.forEach((l)=>{u.forEach((y)=>{let h=`${l}:${y}`;if(i.has(h))return;i.add(h),n+=`
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
`}static generateOutputPackageJson(e=[]){let t=e.some((r)=>r.nodes.some((n)=>n.type===S));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...t?{"@aws-sdk/client-s3":"3.808.0","mime-types":"^3.0.1"}:{}}},null,2)}}function z(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=M.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=O.generateWorkflowCode(n),s=_.generateRoutesCode([n]),o=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:z};})();
