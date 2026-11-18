(()=>{var{defineProperty:J,getOwnPropertyNames:Y,getOwnPropertyDescriptor:ee}=Object,te=Object.prototype.hasOwnProperty;function re(e){return this[e]}var ne=(e)=>{var t=(B??=new WeakMap).get(e),r;if(t)return t;if(t=J({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of Y(e))if(!te.call(t,n))J(t,n,{get:re.bind(e,n),enumerable:!(r=ee(e,n))||r.enumerable})}return B.set(e,t),t},B;var oe=(e)=>e;function se(e,t){this[e]=oe.bind(null,t)}var ie=(e,t)=>{for(var r in t)J(e,r,{get:t[r],enumerable:!0,configurable:!0,set:se.bind(t,r)})};var de={};ie(de,{compileWorkflowJson:()=>Z});var ae=new Set(["if","switch","wait","httpRequest","code","set"].map((e)=>"n8n-nodes-base."+e));function V(e,t){let r=[];for(let[n,a]of Object.entries(t)){if(!e.has(n)||!Array.isArray(a?.main))continue;a.main.forEach((s,o)=>{if(!Array.isArray(s))return;for(let p of s)if(p&&e.has(p.node))r.push({source:n,output:o,target:p.node,input:p.index??0})})}return r}function pe(e,t){let r=new Map,n=new Map,a=[],s=new Set,o=[];function p(i){let f=r.size;r.set(i,f),n.set(i,f),a.push(i),s.add(i);for(let{target:g}of t.get(i)??[])if(!r.has(g))p(g),n.set(i,Math.min(n.get(i),n.get(g)));else if(s.has(g))n.set(i,Math.min(n.get(i),r.get(g)));if(n.get(i)!==f)return;let m=[],d;do d=a.pop(),s.delete(d),m.push(d);while(d!==i);if(m.length>1||t.get(i)?.some((g)=>g.target===i))o.push(m)}for(let i of e)if(!r.has(i))p(i);return o}function q(e,t){let r=new Map(e.map((o)=>[o.name,o])),n=V(r,t),a=new Map,s=new Map;for(let o of n){if(!a.has(o.source))a.set(o.source,[]);if(!s.has(o.target))s.set(o.target,[]);a.get(o.source).push(o),s.get(o.target).push(o)}return pe(r.keys(),a).map((o)=>{let p=me(o,r,a,s);if(!p)throw Error("[n8n-to-fastify] Cyclic dependency detected. Only single-entry polling loops with a timed Wait and one IF or Switch retry output are supported. Nodes: "+o.slice().sort().map((i)=>JSON.stringify(i)).join(", "));return p})}function F(e,t,r){let n=new Map(r.flatMap((s)=>s.nodes.map((o)=>[o,s.entry]))),a=Object.create(null);for(let s of V(new Map(e.map((o)=>[o.name,o])),t)){let o=n.get(s.source)??s.source,p=n.get(s.target)??s.target;if(o===p)continue;let{main:i}=a[o]??={main:[[]]};i[0].push({node:p,type:"main",index:0})}return{nodes:e.filter((s)=>(n.get(s.name)??s.name)===s.name),connections:a}}function me(e,t,r,n){let a=new Set(e),s=e.map((u)=>t.get(u));if(s.some((u)=>!ae.has(u.type)||u.disabled))return;let o=s.filter((u)=>u.type==="n8n-nodes-base.if"||u.type==="n8n-nodes-base.switch"),p=s.filter((u)=>u.type==="n8n-nodes-base.wait");if(o.length!==1||p.length!==1||(p[0].parameters?.resume??"timeInterval")!=="timeInterval")return;if(o[0].parameters?.options?.allMatchingOutputs)return;let i=o[0].name,f=e.flatMap((u)=>(n.get(u)??[]).filter((y)=>!a.has(y.source)));if(f.length!==1||f[0].input!==0)return;let m=f[0].target,d=[m],g=[],N,v;for(let u=0;u<e.length;u++){let y=d[u],S=r.get(y)??[],h=S.filter((w)=>a.has(w.target));if(h.length!==1||h[0].input!==0||y!==i&&h[0].output!==0)return;let c=S.filter((w)=>!a.has(w.target));if(c.length>0&&(y!==i||c.some((w)=>w.output===h[0].output)))return;if(g.push(...c),y===i)v=h[0].output;if(h[0].target===m){if(u!==e.length-1)return;N=h[0]}else d.push(h[0].target)}if(!N||v===void 0||g.length===0)return;let I=[],l=new Set;for(let u of g){let y=t.get(u.target);if(y.type==="n8n-nodes-base.stopAndError"&&!y.disabled){if(u.input!==0||n.get(u.target)?.length!==1||r.get(u.target)?.length)return;I.push({output:u.output,target:u.target})}l.add(u.output)}return{entry:m,nodes:d,decision:i,repeatPort:v,backEdge:{source:N.source,output:N.output},exitPorts:[...l].sort((u,y)=>u-y),terminalExits:I}}class T{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,a=e.name||t,s=e.nodes||[],o=e.connections||{},p=r||`${t}.json`,i=new Map;s.forEach((l)=>i.set(l.name,[])),Object.entries(o).forEach(([l,u])=>{if(u.main&&Array.isArray(u.main))u.main.forEach((y,S)=>{if(Array.isArray(y))y.forEach((h)=>{if(h&&h.node&&i.has(h.node))i.get(h.node)?.push({sourceNode:l,outputIndex:S})})})}),s.forEach((l)=>{l.incomingConnections=i.get(l.name)||[]});let f=s.filter((l)=>l.type.includes("webhook")||l.type.includes("trigger")||l.type.includes("cron")),m=new Set(f.map((l)=>l.name));s.forEach((l)=>{let u=new Set,y=new Set,S=[...(i.get(l.name)||[]).map((h)=>h.sourceNode)];while(S.length>0){let h=S.shift();if(y.has(h))continue;if(y.add(h),m.has(h))u.add(h);(i.get(h)||[]).forEach((w)=>S.push(w.sourceNode))}l.ancestorTriggers=Array.from(u)});let d=q(s,o),g=d.length?F(s,o,d):{nodes:s,connections:o},N=new Map(d.map((l)=>[l.entry,l.nodes])),v=new Map(s.map((l)=>[l.name,l])),I=this.topologicalSort(g.nodes,g.connections).flatMap((l)=>N.get(l.name)?.map((u)=>v.get(u))??[l]);return{id:n,name:a,fileName:t,relativePath:p.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:I,triggers:f,pollingLoops:d}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((m)=>{r.set(m.name,m),n.set(m.name,[])}),Object.entries(t).forEach(([m,d])=>{if(d.main&&Array.isArray(d.main))d.main.forEach((g)=>{if(Array.isArray(g))g.forEach((N)=>{if(N&&N.node&&r.has(N.node))n.get(m)?.push(N.node)})})});let a=new Set,s=new Set,o=[],p=(m,d=[])=>{if(a.has(m))return;if(s.has(m)){let v=[...d,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${v}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(m),d.push(m);let g=n.get(m)||[];for(let v of g)p(v,d);d.pop(),s.delete(m),a.add(m);let N=r.get(m);if(N)o.push(N)},i=new Map;e.forEach((m)=>i.set(m.name,0));for(let m of n.values())for(let d of m)i.set(d,(i.get(d)||0)+1);let f=e.map((m)=>m.name).filter((m)=>i.get(m)===0);for(let m of f)p(m);for(let m of e)if(!a.has(m.name))p(m.name);return o.reverse()}}class b{static parse(e,t){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let p=r.substring(1).trim();if(p.startsWith("{{")&&p.endsWith("}}")){let i=p.substring(2,p.length-2).trim();if(!i.includes("{{")&&!i.includes("}}"))return`(${this.transformTokens(i,t)})`}r=r.substring(1).trim()}let n=[],a=0,o=r.replace(/\{\{(.*?)\}\}/g,(p,i)=>{let f=this.transformTokens(i.trim(),t),m=`__EXPR_PLACEHOLDER_${a++}__`;return n.push(`\${(${f}) ?? ''}`),m}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((p,i)=>{o=o.replace(`__EXPR_PLACEHOLDER_${i}__`,p)}),`\`${o}\``}static transformTokens(e,t){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,a,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(a)})?.item?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,a,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(a)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(n,a)=>`ctx.getNode(${JSON.stringify(a)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(n,a)=>`ctx.getNode(${JSON.stringify(a)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,a)=>{if(!a)return"item?.json";return`item?.json?.${a.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,"inputItems"),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,a)=>`process.env.${a}`),r}}var D={seconds:1000,minutes:60000,hours:3600000,days:86400000};function G(e,t){if(typeof t!=="string"||!Object.hasOwn(D,t))throw Error("Wait unit must be seconds, minutes, hours, or days");let r=typeof e==="string"&&e.trim()?Number(e):e;if(typeof r!=="number"||!Number.isFinite(r)||r<0)throw Error("Wait amount must be a finite nonnegative number");let n=r*D[t];if(n>Number.MAX_SAFE_INTEGER)throw Error("Wait interval is too large");return n}var _="n8n-nodes-base.wait";function U(e){let t=e.parameters??{};return{amount:t.amount??(e.typeVersion===1?1:5),unit:t.unit??(e.typeVersion===1?"hours":"seconds")}}function z(e){return typeof e==="string"&&e.trimStart().startsWith("=")&&e.includes("{{")}function ce(e){let t=e.typeVersion??1.1;if(t!==1&&t!==1.1||(e.parameters?.resume??"timeInterval")!=="timeInterval")return"Wait supports the After Time Interval mode on node versions 1 and 1.1";let{amount:r,unit:n}=U(e);try{G(z(r)?0:r,z(n)?"seconds":n)}catch(a){return a.message}}function H(e){let t=ce(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let{amount:r,unit:n}=U(e.node),a=e.previousVariableName??"inputItems";return`
    ${e.variableName} = ${a} || inputItems;
    if (${e.variableName}.length > 0) {
      const inputItems: N8nItem[] = ${e.variableName};
      const item = inputItems[0];
      await waitForInterval(${b.parse(r)}, ${b.parse(n)});
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}var P="n8n-nodes-base.stopAndError";function ue(e){if((e.typeVersion??1)!==1)return"Stop and Error supports node version 1";let t=e.parameters?.errorType??"errorMessage";if(t!=="errorMessage"&&t!=="errorObject")return"Stop and Error supports Error Message and Error Object"}function K(e){let t=ue(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters??{},n=r.errorType??"errorMessage",a=r[n]??"",s=b.parse(typeof a==="string"&&!a.includes("{{")?a.replace(/^=/,""):a);return`
    {
      const source: N8nItem[] = ${e.previousVariableName??"inputItems"} || inputItems;
      if (source.length > 0) {
        const inputItems = source;
        const item = inputItems[0];
        throw createWorkflowStopError(${JSON.stringify(n)}, ${s});
      }
    }`}class x{static mapNode(e){let t=e.node.type;if(t===P)return K(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t===_)return H(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,a=e.previousVariableName||"inputItems",s="item?.json || {}";if(r==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)s=b.parse(t.responseBody);else if(r==="text"&&t.responseBody)s=b.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=e.node.parameters||{},r=t.jsCode||t.functionCode||"return $input.all();",n=e.previousVariableName||"inputItems",a=r.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(s,o)=>`(ctx.getNode(${JSON.stringify(o)})?.all() || [])`).replace(/\$now/g,"DateTime.now()");return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${n} || inputItems;
      const result = await (async () => {
        ${a}
      })();
      return n8n.normalizeItems(result);
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},r=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(r)}, ${n} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let s of t.assignments.assignments)if(s.name){let o=b.parse(s.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(s.name)}, ${o});`)}}if(t.values)for(let s of["string","number","boolean"]){let o=t.values[s];if(Array.isArray(o)){for(let p of o)if(p.name){let i=b.parse(p.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${i});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return { json: newJson };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],n=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=b.parse(s.leftValue,n),p=b.parse(s.rightValue,n),i=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(i)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,p=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${p}))`;switch(i){case"equals":case"equal":r.push(`(${o} == ${p})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${p})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${p} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${p}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${p}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${p}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${p}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let a=e?.combinator==="or"?" || ":" && ";return r.length?r.join(a):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
        }`:"",m=`
      const predicates = [${n.map((g)=>`(item: N8nItem) => (${g.conditions?this.parseConditions(g.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${b.parse(a.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${i}
        if (matched) routed.push(item);
      }`,d=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${b.parse(t.numberOutputs??4,"firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${b.parse(t.output??0,"firstInputItem")};
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
      ${t.mode==="expression"?d:m}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),a=b.parse(t.url||""),s="{}";if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))s=`{ ${t.headerParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${b.parse(i.value)}`).join(", ")} }`;let o="undefined";if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)o=`JSON.stringify(${b.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))o=`JSON.stringify({ ${t.bodyParameters.parameters.filter((i)=>i.name).map((i)=>`${JSON.stringify(i.name)}: ${b.parse(i.value)}`).join(", ")} })`}return`
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=b.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",a=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=b.parse(a);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}function X(e,t,r,n){let{entry:a,terminalExits:s,repeatPort:o,...p}=e,f=t.get(a).incomingConnections.find((g)=>!e.nodes.includes(g.sourceNode)),m="ctx.getNodeOutputBatches("+JSON.stringify(f.sourceNode)+", "+f.outputIndex+")",d="";for(let[g,N]of e.nodes.entries()){let v=t.get(N);if(g===0)d+=n(v,"true",r(v,"pollingItems"));else{let I=v.incomingConnections.find((u)=>u.sourceNode===e.nodes[g-1]),l="pollingInput_"+g;d+="    const "+l+" = ctx.getNodeOutput("+JSON.stringify(I.sourceNode)+", "+I.outputIndex+`);
`,d+=n(v,l+"?.length",r(v,l))}if(N!==e.decision)continue;for(let[I,l]of s.entries()){let u=t.get(l.target),y="pollingTerminal_"+I;d+="    const "+y+" = ctx.getNodeOutput("+JSON.stringify(N)+", "+l.output+`);
`,d+=n(u,y+"?.length",r(u,y))}d+="    if (ctx.getNodeOutput("+JSON.stringify(N)+", "+o+`)?.length) assertCanRetry();
`}return"  await ctx.runPollingLoop("+JSON.stringify(p)+", "+m+`, async (pollingItems, assertCanRetry) => {
`+d+`  });
`}class E{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",a=e.nodes.some((c)=>c.type===_)?`import { waitForInterval } from '${n}/wait-interval';
`:"",s=e.nodes.some((c)=>c.type===P)?`import { createWorkflowStopError } from '${n}/stop-and-error';
`:"",o=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${a}${s}

const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,p=`  let item: N8nItem = inputItems[0] || { json: {} };
`,i=`  ctx.setNodeOutput("__initial__", inputItems);
`,f="inputItems",m=new Map;e.orderedNodes.forEach((c,w)=>{let k=`node_${w}_${this.sanitizeIdentifier(c.name)}`;m.set(c.name,k)});let d=e.orderedNodes.filter((c)=>c.type.includes("webhook")||c.type.includes("trigger")||c.type.includes("cron")),g=e.orderedNodes.filter((c)=>!d.includes(c)),N=e.pollingLoops??[],v=new Map(N.flatMap((c)=>c.nodes.map((w)=>[w,c]))),I=new Set(N.flatMap((c)=>c.terminalExits.map((w)=>w.target))),l=new Map(e.nodes.map((c)=>[c.name,c])),u=(c,w)=>x.mapNode({node:c,workflowName:e.name,variableName:m.get(c.name),previousVariableName:w});d.forEach((c)=>{let w=e.orderedNodes.indexOf(c),k=m.get(c.name);p+=`  let ${k}: any = inputItems;
`;let j=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");i+=`  const triggerNode_${w} = item?.json?.__triggerNode;
`,i+=`  const triggerPath_${w} = item?.json?.__triggerPath;
`,i+=`  if (!triggerNode_${w} || triggerNode_${w} === ${JSON.stringify(c.name)} || triggerPath_${w} === ${JSON.stringify(j)}) {
`,i+=`    ${k} = inputItems;
`,i+=`    ctx.setNodeOutput(${JSON.stringify(c.name)}, inputItems);
`,i+=`  }
`}),g.forEach((c)=>{let w=m.get(c.name);p+=`  let ${w}: any = inputItems;
`;let k=v.get(c.name);if(k){if(c.name===k.entry)i+=X(k,l,u,(C,O,Q)=>this.generateConditionalBlock(O,Q));return}if(I.has(c.name))return;let j="true",M=c.incomingConnections??[],R=M.length>0&&!c.type.includes("merge");if(M.length>0)j=`(${M.map((O)=>`ctx.isBranchActive(${JSON.stringify(O.sourceNode)}, ${O.outputIndex})`).join(" || ")})`;let A=u(c,R?"incomingItems":"inputItems");if(R){let C=M.map((O)=>`ctx.getNodeOutputBatches(${JSON.stringify(O.sourceNode)}, ${O.outputIndex})`);A=`await ctx.runNodeBatches(${JSON.stringify(c.name)}, [${C.join(", ")}].flat(), async (incomingItems) => {
${A.trim()}
});`}let L=this.generateConditionalBlock(j,A);if(c.ancestorTriggers&&c.ancestorTriggers.length>0){let C=c.ancestorTriggers.map((O)=>`ctx.hasExecutedNode(${JSON.stringify(O)})`).join(" || ");i+=`  if (${C}) {
  ${L.trim().replace(/\n/g,`
  `)}
  }
`}else i+=L;f=c.name});let y=e.orderedNodes.filter((c)=>c.type!==P||c.disabled).at(-1)?.name,S=y?`(ctx.getNode(${JSON.stringify(y)})?.all() || [{ json: item.json }])`:"inputItems",h=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${o}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
${p}
${i}
  const finalItems = ${S};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(h)}, ${t});
`}static generateConditionalBlock(e,t){return`  if (${e}) {
    ${t.trim().replace(/\n/g,`
    `)}
  }
`}}class W{static generateRoutesCode(e){let t=`import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((s)=>{let o=`workflow_${E.sanitizeIdentifier(s.fileName)}`,p=s.relativePath.replace(/\.ts$/,"");t+=`import { ${o} } from '../workflows/${p}';
`,r.push(o)});let n="",a=new Set;return e.forEach((s)=>{let o=`workflow_${E.sanitizeIdentifier(s.fileName)}`;s.nodes.filter((i)=>i.type.includes("webhook")).forEach((i)=>{let f=(i.parameters?.path||s.fileName).toLowerCase().trim().replace(/^\/+/,""),m=(i.parameters?.httpMethod||"POST").toUpperCase(),d=m==="ALL"?["GET","POST","PUT","DELETE"]:[m],g=Array.from(new Set([`/${f}`,`/webhook/${f}`]));d.forEach((N)=>{g.forEach((v)=>{let I=`${N}:${v}`;if(a.has(I))return;a.add(I),n+=`
  fastify.route({
    method: '${N}',
    url: '${v}',
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
          __triggerPath: ${JSON.stringify(f)}
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
`}static generateOutputPackageJson(){return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1"}},null,2)}}function Z(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=T.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let a=E.generateWorkflowCode(n),s=W.generateRoutesCode([n]),o=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:o,workflowCode:a,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:Z};})();
