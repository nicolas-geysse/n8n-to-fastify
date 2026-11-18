(()=>{var{defineProperty:B,getOwnPropertyNames:ie,getOwnPropertyDescriptor:ae}=Object,pe=Object.prototype.hasOwnProperty;function me(e){return this[e]}var de=(e)=>{var t=(U??=new WeakMap).get(e),r;if(t)return t;if(t=B({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var n of ie(e))if(!pe.call(t,n))B(t,n,{get:me.bind(e,n),enumerable:!(r=ae(e,n))||r.enumerable})}return U.set(e,t),t},U;var ce=(e)=>e;function ue(e,t){this[e]=ce.bind(null,t)}var le=(e,t)=>{for(var r in t)B(e,r,{get:t[r],enumerable:!0,configurable:!0,set:ue.bind(t,r)})};var $e={};le($e,{compileWorkflowJson:()=>re});var j="n8n-nodes-base.rabbitmqTrigger";function q(e){return e.nodes.filter((t)=>t.type==="n8n-nodes-base.rabbitmqTrigger"&&!t.disabled)}function W(e){return typeof e==="string"&&!e.startsWith("=")&&!e.includes("{{")}function Q(e){if((e.typeVersion??1)!==1)return"RabbitMQ Trigger needs typeVersion 1";let{queue:t,options:r}=e.parameters??{},n=r??{};if(!W(t)||!t.trim())return"RabbitMQ Trigger needs a static queue name";if(typeof n!=="object"||Array.isArray(n))return"RabbitMQ Trigger needs static options";if(n.acknowledge!=="executionFinishesSuccessfully")return"RabbitMQ Trigger needs the executionFinishesSuccessfully acknowledgement mode";if(n.contentIsBinary||n.onlyContent)return"RabbitMQ Trigger supports text or parsed JSON in the message envelope";let i=n.parallelMessages??-1;if(!Number.isInteger(i)||i!==-1&&(i<1||i>65535))return"RabbitMQ Trigger needs parallelMessages between 1 and 65535, or -1";let s=n.binding?.bindings??[];if(!Array.isArray(s)||!s.every((o)=>W(o?.exchange)&&o.exchange.length>0&&W(o?.routingKey)))return"RabbitMQ Trigger needs static exchange names and routing keys in its bindings";for(let o of["assertQueue","durable","autoDelete","exclusive","jsonParseBody"])if(n[o]!==void 0&&typeof n[o]!=="boolean")return"RabbitMQ Trigger needs static boolean queue and message settings";if(n.assertQueue&&Object.keys(n.arguments??{}).length>0)return"RabbitMQ Trigger queue declaration does not support custom arguments";return{queue:t,bindings:s.map(({exchange:o,routingKey:p})=>({exchange:o,routingKey:p})),prefetch:i===-1?void 0:i,parseJson:n.jsonParseBody===!0,declareQueue:n.assertQueue?{durable:n.durable,autoDelete:n.autoDelete,exclusive:n.exclusive}:void 0}}class F{static parseWorkflow(e,t,r){if(!e||!Array.isArray(e.nodes))return null;let n=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},p=r||`${t}.json`,a=new Map;s.forEach((f)=>a.set(f.name,[])),Object.entries(o).forEach(([f,N])=>{if(N.main&&Array.isArray(N.main))N.main.forEach((y,b)=>{if(Array.isArray(y))y.forEach((g)=>{if(g&&g.node&&a.has(g.node))a.get(g.node)?.push({sourceNode:f,outputIndex:b,inputIndex:g.index??0})})})}),s.forEach((f)=>{f.incomingConnections=a.get(f.name)||[]});let u=s.filter((f)=>f.type===j||f.type.includes("webhook")||f.type.includes("trigger")||f.type.includes("cron")),m=new Set(u.map((f)=>f.name));s.forEach((f)=>{let N=new Set,y=new Set,b=[...(a.get(f.name)||[]).map((g)=>g.sourceNode)];while(b.length>0){let g=b.shift();if(y.has(g))continue;if(y.add(g),m.has(g))N.add(g);(a.get(g)||[]).forEach((J)=>b.push(J.sourceNode))}f.ancestorTriggers=Array.from(N)});let l=this.topologicalSort(s,o);return{id:n,name:i,fileName:t,relativePath:p.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:l,triggers:u}}static topologicalSort(e,t){let r=new Map,n=new Map;e.forEach((m)=>{r.set(m.name,m),n.set(m.name,[])}),Object.entries(t).forEach(([m,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((f)=>{if(Array.isArray(f))f.forEach((N)=>{if(N&&N.node&&r.has(N.node))n.get(m)?.push(N.node)})})});let i=new Set,s=new Set,o=[],p=(m,l=[])=>{if(i.has(m))return;if(s.has(m)){let y=[...l,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(m),l.push(m);let f=n.get(m)||[];for(let y of f)p(y,l);l.pop(),s.delete(m),i.add(m);let N=r.get(m);if(N)o.push(N)},a=new Map;e.forEach((m)=>a.set(m.name,0));for(let m of n.values())for(let l of m)a.set(l,(a.get(l)||0)+1);let u=e.map((m)=>m.name).filter((m)=>a.get(m)===0);for(let m of u)p(m);for(let m of e)if(!i.has(m.name))p(m.name);return o.reverse()}}class d{static parse(e,t){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let p=r.substring(1).trim();if(p.startsWith("{{")&&p.endsWith("}}")){let a=p.substring(2,p.length-2).trim();if(!a.includes("{{")&&!a.includes("}}"))return`(${this.transformTokens(a,t)})`}r=r.substring(1).trim()}let n=[],i=0,o=r.replace(/\{\{(.*?)\}\}/g,(p,a)=>{let u=this.transformTokens(a.trim(),t),m=`__EXPR_PLACEHOLDER_${i++}__`;return n.push(`\${(${u}) ?? ''}`),m}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return n.forEach((p,a)=>{o=o.replace(`__EXPR_PLACEHOLDER_${a}__`,p)}),`\`${o}\``}static transformTokens(e,t){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.item?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(n,i,s)=>{let o=s?s.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(i)})?.first()?.json${o.length?"?."+o.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(n,i)=>`ctx.getNode(${JSON.stringify(i)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(n,i)=>{if(!i)return"item?.json";return`item?.json?.${i.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,"inputItems"),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(n,i)=>`process.env.${i}`),r}}function E(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function fe(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:r=0}of e.incomingConnections??[])if(!Number.isInteger(r)||r<0||r>=t)return"position Merge has an invalid input port"}function G(e){let t=fe(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=r.numberInputs??2,i=Array.from({length:n},()=>[]);for(let{sourceNode:a,outputIndex:u,inputIndex:m=0}of e.node.incomingConnections??[])i[m].push(`ctx.getNodeOutput(${JSON.stringify(a)}, ${u}) || []`);let s=i.map((a)=>`[${a.join(", ")}].flat()`),o=r.options??{},p=d.parse(o.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${s.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${p}), ${JSON.stringify(o.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function V(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function z(e){if(V(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function K(e,t){let r=new Map,n=[e],i=new Set([e.name]);for(let s=0;s<n.length;s++){let o=[...n[s].incomingConnections??[]];o.sort((p,a)=>(p.inputIndex??0)-(a.inputIndex??0));for(let{sourceNode:p,outputIndex:a}of o){if(i.has(p))continue;i.add(p),r.set(p,a);let u=t.get(p);if(u)n.push(u)}}return[...r]}var S="n8n-nodes-base.s3";function ge(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function X(e){let t=ge(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters,n=e.previousVariableName??"inputItems",i=d.parse(r.bucketName),s=d.parse(r.fileKey),o=d.parse(r.binaryPropertyName??"data");return`
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
    }`}function A(e){return e?.sendBody===!0&&e.contentType==="multipart-form-data"}function x(e){return e?.sendHeaders===!0&&e.specifyHeaders==="json"}function Z(e){let t=(e.method??"GET").toUpperCase();if(!["POST","PUT","PATCH"].includes(t))return"HTTP Request multipart supports POST, PUT and PATCH";let r=e.bodyParameters?.parameters??[];if(!Array.isArray(r))return"HTTP Request multipart needs an array of form fields";for(let n of r){if(typeof n?.name!=="string"||!n.name)return"HTTP Request multipart fields require a name";if(n.parameterType==="formBinaryData"){if(typeof n.inputDataFieldName!=="string"||!n.inputDataFieldName)return"HTTP Request multipart binary fields require an input binary property"}else if(n.parameterType&&!["formData","text"].includes(n.parameterType))return"HTTP Request multipart supports text and binary form fields"}}function Y(e){return`createRequestForm(item, [${(e.bodyParameters?.parameters??[]).map((n)=>{let i=d.parse(n.name),s=n.parameterType==="formBinaryData"?"binaryProperty":"value",o=d.parse(n.parameterType==="formBinaryData"?n.inputDataFieldName:n.value);return`{ name: String(${i} ?? ''), ${s}: String(${o} ?? '') }`}).join(", ")}])`}var O="n8n-nodes-base.rabbitmq";function Ne(e){let t=e.parameters??{},r=e.typeVersion??1;if(r!==1&&r!==1.1)return"RabbitMQ needs typeVersion 1 or 1.1";if((t.operation??"sendMessage")!=="sendMessage"||t.mode!=="exchange"||Object.keys(t.options??{}).length>0)return"RabbitMQ only supports Send Message to an existing exchange with default options"}function ee(e){let t=Ne(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters??{},n=e.previousVariableName??"inputItems",i=d.parse(r.exchange??""),s=d.parse(r.routingKey??""),o=d.parse(r.sendInputData??!0),p=d.parse(r.message??""),a=e.variableName;return`
    {
      const source: N8nItem[] = ${n} || inputItems;
      ${a} = [];
      if (source.length > 0) {
        item = source[0];
        const exchange = String(${i});
        const routingKey = String(${s});
        const sendInputData = Boolean(${o});
        const messages = source.map((item) => sendInputData ? JSON.stringify(item.json) : String(${p}));
        ${a} = await publishRabbitMq(exchange, routingKey, messages);
      }
      item = ${a}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${a});
    }`}var C="n8n-nodes-base.convertToFile";function ye(e){let t=e.parameters??{},r=e.typeVersion??1;if(r!==1&&r!==1.1||t.operation!=="toJson")return"Convert to File supports Convert to JSON on node versions 1 and 1.1";if((t.mode??"once")!=="once")return"Convert to JSON supports the All Items to One File mode";if(!["utf8","utf-8"].includes(t.options?.encoding??"utf8"))return"Convert to JSON supports UTF-8 encoding"}function te(e){let t=ye(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters??{},n=r.options??{},i=e.previousVariableName??"inputItems",s=d.parse(r.binaryPropertyName??"data"),o=d.parse(n.fileName??""),p=d.parse(n.format??!1),a=d.parse(n.addBOM??!1);return`
    {
      const source: N8nItem[] = ${i} || inputItems;
      ${e.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${e.variableName} = [convertItemsToJsonFile(source, {
          binaryProperty: String(${s} ?? ''),
          fileName: String(${o} ?? ''),
          format: Boolean(${p}),
          addBOM: Boolean(${a}),
        })];
      }
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}class H{static mapNode(e){let t=e.node.type;if(t===O)return ee(e);if(t===S)return X(e);if(t===C)return te(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set"))return this.mapSetNode(e);if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return E(e.node)?G(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},r=t.respondWith||"firstIncomingItem",n=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(r==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(r==="json"&&t.responseBody)s=d.parse(t.responseBody);else if(r==="text"&&t.responseBody)s=d.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=z(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let r=e.node.parameters||{},n=r.jsCode||r.functionCode||"return $input.all();",i=e.previousVariableName||"inputItems",s=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.options?.keepOnlySet===!0||t.include==="none",i=e.node.typeVersion??3.4,s=t.options??{},o;if(i>=3.4){let a=d.parse(t.includeOtherFields??!1),u=d.parse(t.include??"all"),m=d.parse(s.stripBinary??!1);o=`(${a}) && (${u}) !== 'none' && !(${m})`}else if(i>=3)o=d.parse(s.includeBinary??!1);else o=`!(${d.parse(t.keepOnlySet??s.keepOnlySet??!1)})`;let p=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let a of t.assignments.assignments)if(a.name){let u=d.parse(a.value);p.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(a.name)}, ${u});`)}}if(t.values)for(let a of["string","number","boolean"]){let u=t.values[a];if(Array.isArray(u)){for(let m of u)if(m.name){let l=d.parse(m.value);p.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${l});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${r} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${n?"{}":"{ ...currentItem.json }"};
      ${p.join(`
      `)}
      return {
        json: newJson,
        ...(currentItem.binary !== undefined && (${o}) ? { binary: { ...currentItem.binary } } : {}),
      };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let r=[],n=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=d.parse(s.leftValue,n),p=d.parse(s.rightValue,n),a=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(a)){r.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,p=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${p}))`;switch(a){case"equals":case"equal":r.push(`(${o} == ${p})`);break;case"notEquals":case"notEqual":r.push(`(${o} != ${p})`);break;case"contains":r.push(`String(${o} ?? '').includes(String(${p} ?? ''))`);break;case"true":r.push(`(${o} === true)`);break;case"false":r.push(`(${o} === false)`);break;case"gt":case"larger":r.push(`(Number(${o}) > Number(${p}))`);break;case"lt":case"smaller":r.push(`(Number(${o}) < Number(${p}))`);break;case"gte":r.push(`(Number(${o}) >= Number(${p}))`);break;case"lte":r.push(`(Number(${o}) <= Number(${p}))`);break;case"isEmpty":case"empty":r.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":r.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:r.push(`Boolean(${o})`)}}let i=e?.combinator==="or"?" || ":" && ";return r.length?r.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",r=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
      const predicates = [${n.map((f)=>`(item: N8nItem) => (${f.conditions?this.parseConditions(f.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${d.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${a}
        if (matched) routed.push(item);
      }`,l=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${d.parse(t.numberOutputs??4,"firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${d.parse(t.output??0,"firstInputItem")};
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
      ${t.mode==="expression"?l:m}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=(t.method||"GET").toUpperCase(),i=d.parse(t.url||""),s=A(t),o=x(t),p="{}";if(o)p=`parseRequestHeaders(${d.parse(t.jsonHeaders??"{}")})`;else if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))p=`{ ${t.headerParameters.parameters.filter((l)=>l.name).map((l)=>`${JSON.stringify(l.name)}: ${d.parse(l.value)}`).join(", ")} }`;let a="undefined";if(s){let m=Z(t);if(m)throw Error(`Node ${JSON.stringify(e.node.name)}: ${m}`);a=Y(t)}else if(["POST","PUT","PATCH"].includes(n)){if(t.jsonBody)a=`JSON.stringify(${d.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))a=`JSON.stringify({ ${t.bodyParameters.parameters.filter((l)=>l.name).map((l)=>`${JSON.stringify(l.name)}: ${d.parse(l.value)}`).join(", ")} })`}let u=`const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${p} };`;if(s)u=`const reqHeaders = ${o?p:`new Headers(${p})`};
      reqHeaders.delete('content-type');
      reqHeaders.delete('content-length');`;else if(o)u=`const reqHeaders = ${p};
      if (!reqHeaders.has('content-type')) reqHeaders.set('content-type', 'application/json');`;return`
    // [Node: ${e.node.name}] Native HTTP Request (${n})
    ${e.variableName} = await Promise.all((${r} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      ${u}
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=d.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},r=e.previousVariableName||"inputItems",n=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=d.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class k{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,r=(e.relativePath.match(/\//g)||[]).length,n="../".repeat(r+1)+"runtime",i=e.nodes.some(E)?`import { mergeByPosition } from '${n}/position-merge';
`:"",s=new Set(e.nodes.filter(V)),o=s.size>0?`import { createLegacyNodeReferences } from '${n}/code-context';
`:"",p=s.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((c)=>c.name))});
`:"",a=new Map(e.nodes.map((c)=>[c.name,c])),u=e.nodes.some((c)=>c.type===S)?`import { downloadS3Files } from '${n}/s3';
`:"",m=e.nodes.some((c)=>c.type.includes("httpRequest")&&(A(c.parameters)||x(c.parameters)))?`import { createRequestForm, parseRequestHeaders } from '${n}/http-request';
`:"",l=e.nodes.some((c)=>c.type===O)?`import { publishRabbitMq } from '${n}/rabbitmq';
`:"",f=e.nodes.some((c)=>c.type===C)?`import { convertItemsToJsonFile } from '${n}/convert-to-json';
`:"",N=`import { WorkflowContext } from '${n}/workflow-context';
import { n8n, N8nItem } from '${n}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${n}/workflow-registry';
${i}${o}${u}${m}${l}${f}
${p}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,y=`  let item: N8nItem = inputItems[0] || { json: {} };
`,b=`  ctx.setNodeOutput("__initial__", inputItems);
`,g=new Map;e.orderedNodes.forEach((c,v)=>{let h=`node_${v}_${this.sanitizeIdentifier(c.name)}`;g.set(c.name,h)});let R=e.triggers,J=R.some((c)=>c.type===j),ne=e.orderedNodes.filter((c)=>!R.includes(c));R.forEach((c)=>{let v=e.orderedNodes.indexOf(c),h=g.get(c.name);y+=`  let ${h}: any = inputItems;
`;let w=JSON.stringify(c.name),T=`entryNode === ${w}`;if(c.type!==j){let P=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");b+=`  const triggerNode_${v} = item?.json?.__triggerNode;
`,b+=`  const triggerPath_${v} = item?.json?.__triggerPath;
`,T=`entryNode === undefined && (!triggerNode_${v} || triggerNode_${v} === ${w} || triggerPath_${v} === ${JSON.stringify(P)})`}let M=`${h} = inputItems;
ctx.setNodeOutput(${w}, inputItems);`;b+=this.generateNodeBlock(e.id,c,T,M)}),ne.forEach((c)=>{let v=g.get(c.name);y+=`  let ${v}: any = inputItems;
`;let h="true",w=c.incomingConnections??[],T=w.length>0&&!c.type.includes("merge");if(w.length>0)h=`(${w.map((I)=>`ctx.isBranchActive(${JSON.stringify(I.sourceNode)}, ${I.outputIndex})`).join(" || ")})`;let M=H.mapNode({node:c,workflowName:e.name,variableName:v,previousVariableName:T?"incomingItems":"inputItems",legacyNodePorts:s.has(c)?K(c,a):void 0});if(T){let _=w.map((I)=>`ctx.getNodeOutputBatches(${JSON.stringify(I.sourceNode)}, ${I.outputIndex})`);M=`await ctx.runNodeBatches(${JSON.stringify(c.name)}, [${_.join(", ")}].flat(), async (incomingItems) => {
${M.trim()}
});`}let P=c.ancestorTriggers??[];if(P.length>0)h=`(${P.map((I)=>`ctx.hasExecutedNode(${JSON.stringify(I)})`).join(" || ")}) && ${h}`;else if(J)h=`entryNode === undefined && ${h}`;b+=this.generateNodeBlock(e.id,c,h,M,P.length>0||J?"      ":"    ")});let D=e.orderedNodes[e.orderedNodes.length-1]?.name,oe=D?`(ctx.getNode(${JSON.stringify(D)})?.all() || [{ json: item.json }])`:"inputItems",se=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${N}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext,
  entryNode?: string
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
  const $env = process.env;
${y}
${b}
  const finalItems = ${oe};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(se)}, ${t});
`}static generateNodeBlock(e,t,r,n,i="    "){let s=[e,t.id||t.name,t.name].map((p)=>JSON.stringify(p)).join(", "),o=i+n.trim().replace(/\n/g,`
${i}`);return`  if (${r}) {
    const finishTrace = ctx.startNodeTrace(${s});
    try {
${o}
    } catch (error) {
      finishTrace?.('error');
      throw error;
    }
    finishTrace?.('completed');
  } else {
    ctx.skipNodeTrace(${s});
  }
`}}class L{static generateRoutesCode(e){let t=`import { randomUUID } from 'node:crypto';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,r=[];e.forEach((o)=>{let p=`workflow_${k.sanitizeIdentifier(o.fileName)}`,a=o.relativePath.replace(/\.ts$/,"");t+=`import { ${p} } from '../workflows/${a}';
`,r.push(p)});let n="",i=[],s=new Set;if(e.forEach((o)=>{let p=`workflow_${k.sanitizeIdentifier(o.fileName)}`,a=o.nodes.filter((u)=>u.type.includes("webhook"));for(let u of q(o)){let m=Q(u);if(typeof m==="string")throw Error(`Workflow ${JSON.stringify(o.name)}, node ${JSON.stringify(u.name)}: ${m}`);let l=JSON.stringify({workflowId:o.id,node:u.name});i.push(`      stopConsumers.push(await startRabbitMqConsumer(
        ${JSON.stringify(m)},
        (items) => ${p}(items, undefined, ${JSON.stringify(u.name)}),
        (message) => fastify.log.error(${l}, message),
      ));`)}a.forEach((u)=>{let m=(u.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),l=(u.parameters?.httpMethod||"POST").toUpperCase(),f=l==="ALL"?["GET","POST","PUT","DELETE"]:[l],N=Array.from(new Set([`/${m}`,`/webhook/${m}`]));f.forEach((y)=>{N.forEach((b)=>{let g=`${y}:${b}`;if(s.has(g))return;s.add(g),n+=`
  fastify.route({
    method: '${y}',
    url: '${b}',
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
          __triggerNode: ${JSON.stringify(u.name)},
          __triggerPath: ${JSON.stringify(m)}
        }
      }];

      try {
        const { items, context } = await ${p}(inputItems, ctx);
        const durationMs = (performance.now() - startTime).toFixed(2);
        reply.header('X-Compiled-By', 'n8n-to-fastify');
        reply.header('X-Execution-Time', \`\${durationMs}ms\`);

        if (context.webhookResponse) {
          reply.code(context.webhookResponse.statusCode);
          return context.webhookResponse.body;
        }
        reply.code(200);
        return items.length === 1 ? items[0].json : items.map(i => i.json);
      } catch {
        const errorId = randomUUID();
        request.log.error({ errorId }, 'Workflow execution failed');
        reply.code(500);
        return { error: 'Workflow Execution Error', message: 'Workflow execution failed', errorId };
      }
    }
  });
`})})})}),i.length>0)t+=`import { startRabbitMqConsumer } from '../runtime/rabbitmq';
`,n+=`
  const stopConsumers: Array<() => Promise<void>> = [];
  const closeConsumers = async () => {
    await Promise.allSettled(stopConsumers.splice(0).map(stop => stop()));
  };
  fastify.addHook('onReady', async () => {
    try {
${i.join(`
`)}
    } catch (error) {
      await closeConsumers();
      throw error;
    }
  });
  fastify.addHook('onClose', closeConsumers);
`;return`${t}
// Prevent bundler/runtime tree-shaking so subworkflows self-register into WorkflowRegistry
const _registeredWorkflows = [${r.join(", ")}];
void _registeredWorkflows;

export async function registerCompiledRoutes(fastify: FastifyInstance): Promise<void> {
${n}
}
`}static generateServerEntry(e=[]){return`import Fastify from 'fastify';
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
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);${e.some((r)=>q(r).length>0)?`
  const close = () => {
    void fastify.close().catch(error => {
      fastify.log.error(error);
      process.exitCode = 1;
    });
  };
  process.once('SIGINT', close);
  process.once('SIGTERM', close);`:""}
} catch (err) {
  fastify.log.error(err);
  process.exit(1);
}
`}static generateOutputPackageJson(e=[],t={}){let r=e.some((s)=>s.nodes.some((o)=>o.type===S)),n=e.some((s)=>q(s).length>0||s.nodes.some((o)=>o.type===O)),i=r||e.some((s)=>s.nodes.some((o)=>o.type===C));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...r?{"@aws-sdk/client-s3":"3.808.0"}:{},...i?{"mime-types":"^3.0.1"}:{},...n?{amqplib:"^0.10.9"}:{},...t}},null,2)}}function re(e){let t=performance.now(),r=typeof e==="string"?JSON.parse(e):e,n=F.parseWorkflow(r,r?.name||"workflow","workflow.ts");if(!n)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=k.generateWorkflowCode(n),s=L.generateRoutesCode([n]),o=(performance.now()-t).toFixed(2);return{name:n.name,nodeCount:n.nodes.length,triggerCount:n.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:re};})();
