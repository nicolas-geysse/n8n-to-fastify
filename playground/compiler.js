(()=>{var{defineProperty:J,getOwnPropertyNames:G,getOwnPropertyDescriptor:H}=Object,K=Object.prototype.hasOwnProperty;function X(e){return this[e]}var Z=(e)=>{var t=(x??=new WeakMap).get(e),r;if(t)return t;if(t=J({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of G(e))if(!K.call(t,n))J(t,n,{get:X.bind(e,n),enumerable:!(r=H(e,n))||r.enumerable})}return x.set(e,t),t},x;var Q=(e)=>e;function Y(e,t){this[e]=Q.bind(null,t)}var ee=(e,t)=>{for(var r in t)J(e,r,{get:t[r],enumerable:!0,configurable:!0,set:Y.bind(t,r)})};var re={};ee(re,{compileWorkflowJson:()=>z});class R{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,i=e.name||t,o=e.nodes||[],s=e.connections||{},m=r||`${t}.json`,a=new Map;o.forEach((d)=>a.set(d.name,[])),Object.entries(s).forEach(([d,g])=>{if(g.main&&Array.isArray(g.main))g.main.forEach((N,h)=>{if(Array.isArray(N))N.forEach((y)=>{if(y&&y.node&&a.has(y.node))a.get(y.node)?.push({sourceNode:d,outputIndex:h,inputIndex:y.index??0})})})}),o.forEach((d)=>{d.incomingConnections=a.get(d.name)||[]});let u=o.filter((d)=>d.type.includes("webhook")||d.type.includes("trigger")||d.type.includes("cron")),p=new Set(u.map((d)=>d.name));o.forEach((d)=>{let g=new Set,N=new Set,h=[...(a.get(d.name)||[]).map((y)=>y.sourceNode)];while(h.length>0){let y=h.shift();if(N.has(y))continue;if(N.add(y),p.has(y))g.add(y);(a.get(y)||[]).forEach((E)=>h.push(E.sourceNode))}d.ancestorTriggers=Array.from(g)});let l=this.topologicalSort(o,s);return{id:n,name:i,fileName:t,relativePath:m.replace(/\.json$/,".ts"),nodes:o,connections:s,orderedNodes:l,triggers:u}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((p)=>{r.set(p.name,p),n.set(p.name,[])}),Object.entries(t).forEach(([p,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((d)=>{if(Array.isArray(d))d.forEach((g)=>{if(g&&g.node&&r.has(g.node))n.get(p)?.push(g.node)})})});let i=new Set,o=new Set,s=[],m=(p,l=[])=>{if(i.has(p))return;if(o.has(p)){let N=[...l,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${N}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}o.add(p),l.push(p);let d=n.get(p)||[];for(let N of d)m(N,l);l.pop(),o.delete(p),i.add(p);let g=r.get(p);if(g)s.push(g)},a=new Map;e.forEach((p)=>a.set(p.name,0));for(let p of n.values())for(let l of p)a.set(l,(a.get(l)||0)+1);let u=e.map((p)=>p.name).filter((p)=>a.get(p)===0);for(let p of u)m(p);for(let p of e)if(!i.has(p.name))m(p.name);return s.reverse()}}class f{static parse(e,t,r="inputItems"){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let n=e.trim();if(!n.includes("{{"))return JSON.stringify(e);if(n.startsWith("=")){let a=n.substring(1).trim();if(a.startsWith("{{")&&a.endsWith("}}")){let u=a.substring(2,a.length-2).trim();if(!u.includes("{{")&&!u.includes("}}"))return`(${this.transformTokens(u,t,r)})`}n=n.substring(1).trim()}let i=[],o=0,m=n.replace(/\{\{(.*?)\}\}/g,(a,u)=>{let p=this.transformTokens(u.trim(),t,r),l=`__EXPR_PLACEHOLDER_${o++}__`;return i.push(`\${(${p}) ?? ''}`),l}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return i.forEach((a,u)=>{m=m.replace(`__EXPR_PLACEHOLDER_${u}__`,a)}),`\`${m}\``}static transformTokens(e,t,r){let n=e;if(n=n.replace(/\$now\b/g,"new Date().toISOString()"),n=n.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),n=n.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(i,o,s)=>{let m=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(o)})?.item?.json${m.length?"?."+m.join("?."):""})`}),n=n.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(i,o,s)=>{let m=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(o)})?.first()?.json${m.length?"?."+m.join("?."):""})`}),n=n.replace(/\$\(['"](.*?)['"]\)/g,(i,o)=>`ctx.getNode(${JSON.stringify(o)})`),n=n.replace(/\$node\(['"](.*?)['"]\)/g,(i,o)=>`ctx.getNode(${JSON.stringify(o)})`),n=n.replace(/\$json([a-zA-Z0-9_.]*)/g,(i,o)=>{if(!o)return"item?.json";return`item?.json?.${o.split(".").filter(Boolean).join("?.")}`}),n=n.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)n=n.replace(/\$input\.first\(\)/g,t);return n=n.replace(/\$input\.item\.json/g,"item?.json"),n=n.replace(/\$input\.all\(\)/g,r),n=n.replace(/\$env\.([a-zA-Z0-9_]+)/g,(i,o)=>`process.env.${o}`),n}}function k(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function te(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:r=0}of e.incomingConnections??[])if(!Number.isInteger(r)||r<0||r>=t)return"position Merge has an invalid input port"}function B(e){let t=te(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=r.numberInputs??2,i=Array.from({length:n},()=>[]);for(let{sourceNode:a,outputIndex:u,inputIndex:p=0}of e.node.incomingConnections??[])i[p].push(`ctx.getNodeOutput(${JSON.stringify(a)}, ${u}) || []`);let o=i.map((a)=>`[${a.join(", ")}].flat()`),s=r.options??{},m=f.parse(s.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${o.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${m}), ${JSON.stringify(s.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function _(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function q(e){if(_(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function L(e,t){let r=new Map,n=[e],i=new Set([e.name]);for(let o=0;o<n.length;o++){let s=[...n[o].incomingConnections??[]];s.sort((m,a)=>(m.inputIndex??0)-(a.inputIndex??0));for(let{sourceNode:m,outputIndex:a}of s){if(i.has(m))continue;i.add(m),r.set(m,a);let u=t.get(m);if(u)n.push(u)}}return[...r]}function O(e){return e.type==="n8n-nodes-base.set"&&e.parameters?.mode==="raw"}function F(e){let t=e.parameters?.jsonOutput;return O(e)&&typeof t==="string"&&t.startsWith("=")&&/\{\{[\s\S]*?\$node\b[\s\S]*?\}\}/.test(t)}function V(e){if(!O(e))return;if(e.typeVersion!==3.4)return"Edit Fields JSON Output requires node version 3.4";let t=e.parameters,{dotNotation:r,stripBinary:n}=t.options??{};if([t.includeOtherFields,r,n].some((i)=>i!==void 0&&typeof i!=="boolean"))return"Edit Fields JSON Output requires static field and binary options";if(t.includeOtherFields!==!1&&t.includeOtherFields!==void 0&&!["all","none"].includes(t.include??"all"))return"Edit Fields JSON Output supports including all or none of the input fields";if(t.duplicateItem)return"Edit Fields JSON Output does not support manual item duplication"}function ne(e){if(typeof e!=="string"||!e.startsWith("="))return JSON.stringify(e);let t=e.replace(/^=+/,""),r=(m)=>f.parse("="+m,"source[0]","source"),n=t.trim();if(n.startsWith("{{")&&n.endsWith("}}")&&!n.slice(2,-2).includes("{{"))return r(n);let i=[],o=0,s=!1;for(let m of t.matchAll(/\{\{([\s\S]*?)\}\}/g)){let a=t.slice(o,m.index);for(let[u]of a.matchAll(/\\[\s\S]|"/g))if(u==='"')s=!s;i.push(JSON.stringify(a)),i.push("encodeJsonTemplateValue("+r(m[0])+", "+s+")"),o=m.index+m[0].length}return i.push(JSON.stringify(t.slice(o))),"["+i.join(", ")+"].join('')"}function D(e){let t=e.node.parameters,r=t.options??{},n=ne(t.jsonOutput===void 0?{my_field_1:"value",my_field_2:1}:t.jsonOutput),i=t.includeOtherFields===!0&&(t.include??"all")==="all",o=r.stripBinary??!0,s=r.dotNotation??!0,m=e.legacyNodePorts?"const $node = createLegacyNodeReferences(ctx, legacyNodeNames, "+JSON.stringify(e.legacyNodePorts)+", itemIndex);":"";return["{","  const source: N8nItem[] = "+(e.previousVariableName??"inputItems")+";","  "+e.variableName+" = source.map((item, itemIndex) => {",...m?["    "+m]:[],"    return applyRawEditFields(item, itemIndex, "+n+", {","      includeAll: "+i+", stripBinary: "+o+",","      dotNotation: "+s+",","    });","  });","}","item = "+e.variableName+"[0] || { json: {} };","ctx.setNodeOutput("+JSON.stringify(e.node.name)+", "+e.variableName+");"].join(`
`)}class A{static mapNode(e){let t=e.node.type;if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set")){if(O(e.node)&&!V(e.node))return D(e);return this.mapSetNode(e)}if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return k(e.node)?B(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",o="item?.json || {}";if(r==="allIncomingItems")o=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)o=f.parse(t.responseBody);else if(r==="text"&&t.responseBody)o=f.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=q(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters||{},n=r.jsCode||r.functionCode||"return $input.all();",i=e.previousVariableName||"inputItems",o=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
`:"",s=n.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(m,a)=>`(ctx.getNode(${JSON.stringify(a)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(m,a)=>`(ctx.getNode(${JSON.stringify(a)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(m,a)=>`(ctx.getNode(${JSON.stringify(a)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",i=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let o of t.assignments.assignments)if(o.name){let s=f.parse(o.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(o.name)}, ${s});`)}}if(t.values)for(let o of["string","number","boolean"]){let s=t.values[o];if(Array.isArray(s)){for(let m of s)if(m.name){let a=f.parse(m.value);i.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${a});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${i.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],n=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let o of e.conditions){let s=f.parse(o.leftValue,n),m=f.parse(o.rightValue,n),a=o.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(a)){r.push(`Boolean(${s})`);continue}if(!t&&e.options?.caseSensitive===!1&&o.operator?.type==="string")s=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${s}))`,m=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${m}))`;switch(a){case"equals":case"equal":r.push(`(${s} == ${m})`);break;case"notEquals":case"notEqual":r.push(`(${s} != ${m})`);break;case"contains":r.push(`String(${s} ?? '').includes(String(${m} ?? ''))`);break;case"true":r.push(`(${s} === true)`);break;case"false":r.push(`(${s} === false)`);break;case"gt":case"larger":r.push(`(Number(${s}) > Number(${m}))`);break;case"lt":case"smaller":r.push(`(Number(${s}) < Number(${m}))`);break;case"gte":r.push(`(Number(${s}) >= Number(${m}))`);break;case"lte":r.push(`(Number(${s}) <= Number(${m}))`);break;case"isEmpty":case"empty":r.push(`(${s} === undefined || ${s} === null || ${s} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${s} !== undefined && ${s} !== null && ${s} !== '')`);break;default:r.push(`Boolean(${s})`)}}let i=e?.combinator==="or"?" || ":" && ";return r.length?r.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.rules?.values||[],i=t.options??{},o=i.fallbackOutput??"none",s=o==="extra"?n.length:o,a=o==="extra"||Number.isInteger(s)&&s>=0&&s<n.length?`
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
            if (!(${f.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${a}
        if (matched) routed.push(item);
      }`,l=`
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
      ${t.mode==="expression"?l:p}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),i=f.parse(t.url||""),o="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))o=`{ ${t.headerParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${f.parse(a.value)}`).join(", ")} }`;let s="undefined";if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)s=`JSON.stringify(${f.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))s=`JSON.stringify({ ${t.bodyParameters.parameters.filter((a)=>a.name).map((a)=>`${JSON.stringify(a.name)}: ${f.parse(a.value)}`).join(", ")} })`}return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",o=f.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class S{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",i=e.nodes.some(k)?`import { mergeByPosition } from '${n}/position-merge';
`:"",o=new Set(e.nodes.filter((c)=>_(c)||F(c))),s=e.nodes.some(O)?`import { applyRawEditFields, encodeJsonTemplateValue } from '${n}/raw-edit-fields';
`:"",m=o.size>0?`import { createLegacyNodeReferences } from '${n}/code-context';
`:"",a=o.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((c)=>c.name))});
`:"",u=new Map(e.nodes.map((c)=>[c.name,c])),p=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${i}${m}${s}
${a}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,l=`  let item: N8nItem = inputItems[0] || { json: {} };
`,d=`  ctx.setNodeOutput("__initial__", inputItems);
`,g="inputItems",N=new Map;e.orderedNodes.forEach((c,b)=>{let w=`node_${b}_${this.sanitizeIdentifier(c.name)}`;N.set(c.name,w)});let h=e.orderedNodes.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),y=e.orderedNodes.filter((c)=>!h.includes(c));h.forEach((c)=>{let b=e.orderedNodes.indexOf(c),w=N.get(c.name);l+=`  let ${w}: any = inputItems;
`;let I=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");d+=`  const triggerNode_${b} = item?.json?.__triggerNode;
`,d+=`  const triggerPath_${b} = item?.json?.__triggerPath;
`,d+=`  if (!triggerNode_${b} || triggerNode_${b} === ${JSON.stringify(c.name)} || triggerPath_${b} === ${JSON.stringify(I)}) {
`,d+=`    ${w} = inputItems;
`,d+=`    ctx.setNodeOutput(${JSON.stringify(c.name)}, inputItems);
`,d+=`  }
`}),y.forEach((c)=>{let b=N.get(c.name);l+=`  let ${b}: any = inputItems;
`;let w="true",I=c.incomingConnections??[],T=I.length>0&&!c.type.includes("merge");if(I.length>0)w=`(${I.map((v)=>`ctx.isBranchActive(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`).join(" || ")})`;let P=A.mapNode({node:c,workflowName:e.name,variableName:b,previousVariableName:T?"incomingItems":"inputItems",legacyNodePorts:o.has(c)?L(c,u):void 0});if(T){let C=I.map((v)=>`ctx.getNodeOutputBatches(${JSON.stringify(v.sourceNode)}, ${v.outputIndex})`);P=`await ctx.runNodeBatches(${JSON.stringify(c.name)}, [${C.join(", ")}].flat(), async (incomingItems) => {
${P.trim()}
});`}let W=`  if (${w}) {
    ${P.trim().replace(/\n/g,`
    `)}
  }
`;if(c.ancestorTriggers&&c.ancestorTriggers.length>0){let C=c.ancestorTriggers.map((v)=>`ctx.hasExecutedNode(${JSON.stringify(v)})`).join(" || ");d+=`  if (${C}) {
  ${W.trim().replace(/\n/g,`
  `)}
  }
`}else d+=W;g=c.name});let j=e.orderedNodes[e.orderedNodes.length-1]?.name,E=j?`(ctx.getNode(${JSON.stringify(j)})?.all() || [{ json: item.json }])`:"inputItems",U=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${p}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${l}
${d}
  const finalItems = ${E};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(U)}, ${t});
`}}class M{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`,m=o.relativePath.replace(/\.ts$/,"");t+=`import { ${s} } from '../workflows/${m}';
`,r.push(s)});let n="",i=new Set;return e.forEach((o)=>{let s=`workflow_${S.sanitizeIdentifier(o.fileName)}`;o.nodes.filter((a)=>a.type.includes("webhook")).forEach((a)=>{let u=(a.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),p=(a.parameters?.httpMethod||"POST").toUpperCase(),l=p==="ALL"?["GET","POST","PUT","DELETE"]:[p],d=Array.from(new Set([`/${u}`,`/webhook/${u}`]));l.forEach((g)=>{d.forEach((N)=>{let h=`${g}:${N}`;if(i.has(h))return;i.add(h),n+=`
  fastify.route({
    method: '${g}',
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
          __triggerPath: ${JSON.stringify(u)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function z(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=R.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=S.generateWorkflowCode(n),o=M.generateRoutesCode([n]),s=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:s,workflowCode:i,routesCode:o}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:z};})();
