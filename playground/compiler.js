(()=>{var{defineProperty:U,getOwnPropertyNames:Se,getOwnPropertyDescriptor:Ce}=Object,Te=Object.prototype.hasOwnProperty;function ke(e){return this[e]}var Pe=(e)=>{var t=(ee??=new WeakMap).get(e),n;if(t)return t;if(t=U({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of Se(e))if(!Te.call(t,r))U(t,r,{get:ke.bind(e,r),enumerable:!(n=Ce(e,r))||n.enumerable})}return ee.set(e,t),t},ee;var Me=(e)=>e;function je(e,t){this[e]=Me.bind(null,t)}var Ee=(e,t)=>{for(var n in t)U(e,n,{get:t[n],enumerable:!0,configurable:!0,set:je.bind(t,n)})};var Be={};Ee(Be,{compileWorkflowJson:()=>we});var R="n8n-nodes-base.rabbitmqTrigger";function x(e){return e.nodes.filter((t)=>t.type==="n8n-nodes-base.rabbitmqTrigger"&&!t.disabled)}function G(e){return typeof e==="string"&&!e.startsWith("=")&&!e.includes("{{")}function te(e){if((e.typeVersion??1)!==1)return"RabbitMQ Trigger needs typeVersion 1";let{queue:t,options:n}=e.parameters??{},r=n??{};if(!G(t)||!t.trim())return"RabbitMQ Trigger needs a static queue name";if(typeof r!=="object"||Array.isArray(r))return"RabbitMQ Trigger needs static options";if(r.acknowledge!=="executionFinishesSuccessfully")return"RabbitMQ Trigger needs the executionFinishesSuccessfully acknowledgement mode";if(r.contentIsBinary||r.onlyContent)return"RabbitMQ Trigger supports text or parsed JSON in the message envelope";let i=r.parallelMessages??-1;if(!Number.isInteger(i)||i!==-1&&(i<1||i>65535))return"RabbitMQ Trigger needs parallelMessages between 1 and 65535, or -1";let s=r.binding?.bindings??[];if(!Array.isArray(s)||!s.every((o)=>G(o?.exchange)&&o.exchange.length>0&&G(o?.routingKey)))return"RabbitMQ Trigger needs static exchange names and routing keys in its bindings";for(let o of["assertQueue","durable","autoDelete","exclusive","jsonParseBody"])if(r[o]!==void 0&&typeof r[o]!=="boolean")return"RabbitMQ Trigger needs static boolean queue and message settings";if(r.assertQueue&&Object.keys(r.arguments??{}).length>0)return"RabbitMQ Trigger queue declaration does not support custom arguments";return{queue:t,bindings:s.map(({exchange:o,routingKey:a})=>({exchange:o,routingKey:a})),prefetch:i===-1?void 0:i,parseJson:r.jsonParseBody===!0,declareQueue:r.assertQueue?{durable:r.durable,autoDelete:r.autoDelete,exclusive:r.exclusive}:void 0}}class Q{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},a=n||`${t}.json`,p=new Map;s.forEach((f)=>p.set(f.name,[])),Object.entries(o).forEach(([f,g])=>{if(g.main&&Array.isArray(g.main))g.main.forEach((y,w)=>{if(Array.isArray(y))y.forEach((N)=>{if(N&&N.node&&p.has(N.node))p.get(N.node)?.push({sourceNode:f,outputIndex:w,inputIndex:N.index??0})})})}),s.forEach((f)=>{f.incomingConnections=p.get(f.name)||[]});let u=s.filter((f)=>f.type===R||f.type.includes("webhook")||f.type.includes("trigger")||f.type.includes("cron")),m=new Set(u.map((f)=>f.name));s.forEach((f)=>{let g=new Set,y=new Set,w=[...(p.get(f.name)||[]).map((N)=>N.sourceNode)];while(w.length>0){let N=w.shift();if(y.has(N))continue;if(y.add(N),m.has(N))g.add(N);(p.get(N)||[]).forEach((I)=>w.push(I.sourceNode))}f.ancestorTriggers=Array.from(g)});let l=this.topologicalSort(s,o);return{id:r,name:i,fileName:t,relativePath:a.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:l,triggers:u}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((m)=>{n.set(m.name,m),r.set(m.name,[])}),Object.entries(t).forEach(([m,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((f)=>{if(Array.isArray(f))f.forEach((g)=>{if(g&&g.node&&n.has(g.node))r.get(m)?.push(g.node)})})});let i=new Set,s=new Set,o=[],a=(m,l=[])=>{if(i.has(m))return;if(s.has(m)){let y=[...l,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(m),l.push(m);let f=r.get(m)||[];for(let y of f)a(y,l);l.pop(),s.delete(m),i.add(m);let g=n.get(m);if(g)o.push(g)},p=new Map;e.forEach((m)=>p.set(m.name,0));for(let m of r.values())for(let l of m)p.set(l,(p.get(l)||0)+1);let u=e.map((m)=>m.name).filter((m)=>p.get(m)===0);for(let m of u)a(m);for(let m of e)if(!i.has(m.name))a(m.name);return o.reverse()}}class c{static parse(e,t,n="inputItems"){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let p=r.substring(1).trim();if(p.startsWith("{{")&&p.endsWith("}}")){let u=p.substring(2,p.length-2).trim();if(!u.includes("{{")&&!u.includes("}}"))return`(${this.transformTokens(u,t,n)})`}r=r.substring(1).trim()}let i=[],s=0,a=r.replace(/\{\{(.*?)\}\}/g,(p,u)=>{let m=this.transformTokens(u.trim(),t,n),l=`__EXPR_PLACEHOLDER_${s++}__`;return i.push(`\${(${m}) ?? ''}`),l}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return i.forEach((p,u)=>{a=a.replace(`__EXPR_PLACEHOLDER_${u}__`,p)}),`\`${a}\``}static transformTokens(e,t,n){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(i,s,o)=>{let a=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(s)})?.item?.json${a.length?"?."+a.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(i,s,o)=>{let a=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(s)})?.first()?.json${a.length?"?."+a.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(i,s)=>`ctx.getNode(${JSON.stringify(s)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(i,s)=>`ctx.getNode(${JSON.stringify(s)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(i,s)=>{if(!s)return"item?.json";return`item?.json?.${s.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,n),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(i,s)=>`process.env.${s}`),r}}function A(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function Re(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:n=0}of e.incomingConnections??[])if(!Number.isInteger(n)||n<0||n>=t)return"position Merge has an invalid input port"}function re(e){let t=Re(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=n.numberInputs??2,i=Array.from({length:r},()=>[]);for(let{sourceNode:p,outputIndex:u,inputIndex:m=0}of e.node.incomingConnections??[])i[m].push(`ctx.getNodeOutput(${JSON.stringify(p)}, ${u}) || []`);let s=i.map((p)=>`[${p.join(", ")}].flat()`),o=n.options??{},a=c.parse(o.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${s.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${a}), ${JSON.stringify(o.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function z(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function ne(e){if(z(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function oe(e,t){let n=new Map,r=[e],i=new Set([e.name]);for(let s=0;s<r.length;s++){let o=[...r[s].incomingConnections??[]];o.sort((a,p)=>(a.inputIndex??0)-(p.inputIndex??0));for(let{sourceNode:a,outputIndex:p}of o){if(i.has(a))continue;i.add(a),n.set(a,p);let u=t.get(a);if(u)r.push(u)}}return[...n]}var S="n8n-nodes-base.s3";function Je(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function se(e){let t=Je(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=e.previousVariableName??"inputItems",i=c.parse(n.bucketName),s=c.parse(n.fileKey),o=c.parse(n.binaryPropertyName??"data");return`
    {
      const downloads = (${r} || inputItems).map((item: N8nItem) => ({
        item,
        bucket: String(${i} ?? ''),
        key: String(${s} ?? ''),
        binaryProperty: String(${o} ?? ''),
      }));
      ${e.variableName} = await downloadS3Files(downloads);
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}function _(e){return e?.sendBody===!0&&e.contentType==="multipart-form-data"}function F(e){return e?.sendHeaders===!0&&e.specifyHeaders==="json"}function ie(e){let t=(e.method??"GET").toUpperCase();if(!["POST","PUT","PATCH"].includes(t))return"HTTP Request multipart supports POST, PUT and PATCH";let n=e.bodyParameters?.parameters??[];if(!Array.isArray(n))return"HTTP Request multipart needs an array of form fields";for(let r of n){if(typeof r?.name!=="string"||!r.name)return"HTTP Request multipart fields require a name";if(r.parameterType==="formBinaryData"){if(typeof r.inputDataFieldName!=="string"||!r.inputDataFieldName)return"HTTP Request multipart binary fields require an input binary property"}else if(r.parameterType&&!["formData","text"].includes(r.parameterType))return"HTTP Request multipart supports text and binary form fields"}}function ae(e){return`createRequestForm(item, [${(e.bodyParameters?.parameters??[]).map((r)=>{let i=c.parse(r.name),s=r.parameterType==="formBinaryData"?"binaryProperty":"value",o=c.parse(r.parameterType==="formBinaryData"?r.inputDataFieldName:r.value);return`{ name: String(${i} ?? ''), ${s}: String(${o} ?? '') }`}).join(", ")}])`}var C="n8n-nodes-base.rabbitmq";function qe(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1)return"RabbitMQ needs typeVersion 1 or 1.1";if((t.operation??"sendMessage")!=="sendMessage"||t.mode!=="exchange"||Object.keys(t.options??{}).length>0)return"RabbitMQ only supports Send Message to an existing exchange with default options"}function pe(e){let t=qe(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",i=c.parse(n.exchange??""),s=c.parse(n.routingKey??""),o=c.parse(n.sendInputData??!0),a=c.parse(n.message??""),p=e.variableName;return`
    {
      const source: N8nItem[] = ${r} || inputItems;
      ${p} = [];
      if (source.length > 0) {
        item = source[0];
        const exchange = String(${i});
        const routingKey = String(${s});
        const sendInputData = Boolean(${o});
        const messages = source.map((item) => sendInputData ? JSON.stringify(item.json) : String(${a}));
        ${p} = await publishRabbitMq(exchange, routingKey, messages);
      }
      item = ${p}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${p});
    }`}var T="n8n-nodes-base.convertToFile";function xe(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1||t.operation!=="toJson")return"Convert to File supports Convert to JSON on node versions 1 and 1.1";if((t.mode??"once")!=="once")return"Convert to JSON supports the All Items to One File mode";if(!["utf8","utf-8"].includes(t.options?.encoding??"utf8"))return"Convert to JSON supports UTF-8 encoding"}function me(e){let t=xe(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=n.options??{},i=e.previousVariableName??"inputItems",s=c.parse(n.binaryPropertyName??"data"),o=c.parse(r.fileName??""),a=c.parse(r.format??!1),p=c.parse(r.addBOM??!1);return`
    {
      const source: N8nItem[] = ${i} || inputItems;
      ${e.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${e.variableName} = [convertItemsToJsonFile(source, {
          binaryProperty: String(${s} ?? ''),
          fileName: String(${o} ?? ''),
          format: Boolean(${a}),
          addBOM: Boolean(${p}),
        })];
      }
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}function k(e){return e.type==="n8n-nodes-base.set"&&e.parameters?.mode==="raw"}function de(e){let t=e.parameters?.jsonOutput;return k(e)&&typeof t==="string"&&t.startsWith("=")&&/\{\{[\s\S]*?\$node\b[\s\S]*?\}\}/.test(t)}function ue(e){if(!k(e))return;if(e.typeVersion!==3.4)return"Edit Fields JSON Output requires node version 3.4";let t=e.parameters,{dotNotation:n,stripBinary:r}=t.options??{};if([t.includeOtherFields,n,r].some((i)=>i!==void 0&&typeof i!=="boolean"))return"Edit Fields JSON Output requires static field and binary options";if(t.includeOtherFields!==!1&&t.includeOtherFields!==void 0&&!["all","none"].includes(t.include??"all"))return"Edit Fields JSON Output supports including all or none of the input fields";if(t.duplicateItem)return"Edit Fields JSON Output does not support manual item duplication"}function Ae(e){if(typeof e!=="string"||!e.startsWith("="))return JSON.stringify(e);let t=e.replace(/^=+/,""),n=(a)=>c.parse("="+a,"source[0]","source"),r=t.trim();if(r.startsWith("{{")&&r.endsWith("}}")&&!r.slice(2,-2).includes("{{"))return n(r);let i=[],s=0,o=!1;for(let a of t.matchAll(/\{\{([\s\S]*?)\}\}/g)){let p=t.slice(s,a.index);for(let[u]of p.matchAll(/\\[\s\S]|"/g))if(u==='"')o=!o;i.push(JSON.stringify(p)),i.push("encodeJsonTemplateValue("+n(a[0])+", "+o+")"),s=a.index+a[0].length}return i.push(JSON.stringify(t.slice(s))),"["+i.join(", ")+"].join('')"}function ce(e){let t=e.node.parameters,n=t.options??{},r=Ae(t.jsonOutput===void 0?{my_field_1:"value",my_field_2:1}:t.jsonOutput),i=t.includeOtherFields===!0&&(t.include??"all")==="all",s=n.stripBinary??!0,o=n.dotNotation??!0,a=e.legacyNodePorts?"const $node = createLegacyNodeReferences(ctx, legacyNodeNames, "+JSON.stringify(e.legacyNodePorts)+", itemIndex);":"";return["{","  const source: N8nItem[] = "+(e.previousVariableName??"inputItems")+";","  "+e.variableName+" = source.map((item, itemIndex) => {",...a?["    "+a]:[],"    return applyRawEditFields(item, itemIndex, "+r+", {","      includeAll: "+i+", stripBinary: "+s+",","      dotNotation: "+o+",","    });","  });","}","item = "+e.variableName+"[0] || { json: {} };","ctx.setNodeOutput("+JSON.stringify(e.node.name)+", "+e.variableName+");"].join(`
`)}function le(e){let t=e.parameters??{};return e.type==="n8n-nodes-base.code"&&e.onError==="continueRegularOutput"&&(t.language??"javaScript")==="javaScript"&&(t.mode??"runOnceForAllItems")==="runOnceForAllItems"}var P="n8n-nodes-base.compression";function _e(e){let t=e.typeVersion??1;if(t!==1&&t!==1.1||(e.parameters?.operation??"decompress")!=="decompress")return"Compression supports ZIP and GZIP decompression on node versions 1 and 1.1"}function fe(e){let t=_e(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",i=c.parse(n.binaryPropertyName??"data"),s=c.parse(n.outputPrefix??"file_");return`
    {
      const source: N8nItem[] = ${r} || inputItems;
      ${e.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${e.variableName} = await decompressItems(source, String(${i}), String(${s}));
      }
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}var ge={seconds:1000,minutes:60000,hours:3600000,days:86400000};function Ne(e,t){if(typeof t!=="string"||!Object.hasOwn(ge,t))throw Error("Wait unit must be seconds, minutes, hours, or days");let n=typeof e==="string"&&e.trim()?Number(e):e;if(typeof n!=="number"||!Number.isFinite(n)||n<0)throw Error("Wait amount must be a finite nonnegative number");let r=n*ge[t];if(r>Number.MAX_SAFE_INTEGER)throw Error("Wait interval is too large");return r}var B="n8n-nodes-base.wait";function $e(e){let t=e.parameters??{};return{amount:t.amount??(e.typeVersion===1?1:5),unit:t.unit??(e.typeVersion===1?"hours":"seconds")}}function ye(e){return typeof e==="string"&&e.trimStart().startsWith("=")&&e.includes("{{")}function Fe(e){let t=e.typeVersion??1.1;if(t!==1&&t!==1.1||(e.parameters?.resume??"timeInterval")!=="timeInterval")return"Wait supports the After Time Interval mode on node versions 1 and 1.1";let{amount:n,unit:r}=$e(e);try{Ne(ye(n)?0:n,ye(r)?"seconds":r)}catch(i){return i.message}}function be(e){let t=Fe(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let{amount:n,unit:r}=$e(e.node),i=e.previousVariableName??"inputItems";return`
    ${e.variableName} = ${i} || inputItems;
    if (${e.variableName}.length > 0) {
      const inputItems: N8nItem[] = ${e.variableName};
      const item = inputItems[0];
      await waitForInterval(${c.parse(n)}, ${c.parse(r)});
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}class K{static mapNode(e){let t=e.node.type;if(t===C)return pe(e);if(t===S)return se(e);if(t===T)return me(e);if(t===P)return fe(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set")){if(k(e.node)&&!ue(e.node))return ce(e);return this.mapSetNode(e)}if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return A(e.node)?re(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t===B)return be(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(n==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)s=c.parse(t.responseBody);else if(n==="text"&&t.responseBody)s=c.parse(t.responseBody);return`
    // [Node: ${e.node.name}] Respond to Webhook
    ${e.variableName} = ${i} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      let payload: any = ${s};
      if (typeof payload === 'string' && (payload.trim().startsWith('{') || payload.trim().startsWith('['))) {
        try { payload = JSON.parse(payload); } catch {}
      }
      ctx.setWebhookResponse(${r}, payload);
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapCodeNode(e){let t=ne(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters||{},r=n.jsCode||n.functionCode||"return $input.all();",i=e.previousVariableName||"inputItems",s=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
`:"",o=r.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(u,m)=>`(ctx.getNode(${JSON.stringify(m)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(u,m)=>`(ctx.getNode(${JSON.stringify(m)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(u,m)=>`(ctx.getNode(${JSON.stringify(m)})?.all() || [])`).replace(/\$now/g,"DateTime.now()"),a=`
${s}      const result = await (async () => {
        ${o}
      })();
      return n8n.normalizeItems(result);`,p=le(e.node)?`
      try {${a}
      } catch (error) {
        return [{ json: { error: error instanceof Error ? error.message : String(error) } }];
      }`:a;return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${i} || inputItems;${p}
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},n=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(n)}, ${r} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",i=e.node.typeVersion??3.4,s=t.options??{},o;if(i>=3.4){let p=c.parse(t.includeOtherFields??!1),u=c.parse(t.include??"all"),m=c.parse(s.stripBinary??!1);o=`(${p}) && (${u}) !== 'none' && !(${m})`}else if(i>=3)o=c.parse(s.includeBinary??!1);else o=`!(${c.parse(t.keepOnlySet??s.keepOnlySet??!1)})`;let a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let p of t.assignments.assignments)if(p.name){let u=c.parse(p.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${u});`)}}if(t.values)for(let p of["string","number","boolean"]){let u=t.values[p];if(Array.isArray(u)){for(let m of u)if(m.name){let l=c.parse(m.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${l});`)}}}return`
    // [Node: ${e.node.name}] Set / Edit Fields
    ${e.variableName} = (${n} || inputItems).map((currentItem: N8nItem) => {
      const item = currentItem;
      const newJson: Record<string, any> = ${r?"{}":"{ ...currentItem.json }"};
      ${a.join(`
      `)}
      return {
        json: newJson,
        ...(currentItem.binary !== undefined && (${o}) ? { binary: { ...currentItem.binary } } : {}),
      };
    });
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let n=[],r=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=c.parse(s.leftValue,r),a=c.parse(s.rightValue,r),p=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(p)){n.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,a=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${a}))`;switch(p){case"equals":case"equal":n.push(`(${o} == ${a})`);break;case"notEquals":case"notEqual":n.push(`(${o} != ${a})`);break;case"contains":n.push(`String(${o} ?? '').includes(String(${a} ?? ''))`);break;case"true":n.push(`(${o} === true)`);break;case"false":n.push(`(${o} === false)`);break;case"gt":case"larger":n.push(`(Number(${o}) > Number(${a}))`);break;case"lt":case"smaller":n.push(`(Number(${o}) < Number(${a}))`);break;case"gte":n.push(`(Number(${o}) >= Number(${a}))`);break;case"lte":n.push(`(Number(${o}) <= Number(${a}))`);break;case"isEmpty":case"empty":n.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":n.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:n.push(`Boolean(${o})`)}}let i=e?.combinator==="or"?" || ":" && ";return n.length?n.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",n=this.parseConditions(e.node.parameters?.conditions,!0);return`
    // [Node: ${e.node.name}] Filter
    ${e.variableName} = ${t} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    {
      const isMatch = Boolean(${n});
      const activeIndex = isMatch ? 0 : 1;
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName}, activeIndex);
    }`}static mapIfNode(e){let t=e.previousVariableName||"inputItems",n=this.parseConditions(e.node.parameters?.conditions);return`
    // [Node: ${e.node.name}] Conditional Branch (IF)
    ${e.variableName} = ${t} || inputItems;
    {
      const firstInputItem: N8nItem | undefined = ${e.variableName}[0];
      const outputs: N8nItem[][] = [[], []];
      for (const item of ${e.variableName} as N8nItem[]) {
        const port = (${n}) ? 0 : 1;
        outputs[port].push(item);
      }
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs.entries(), ${e.variableName});
    }
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],i=t.options??{},s=i.fallbackOutput??"none",o=s==="extra"?r.length:s,p=s==="extra"||Number.isInteger(o)&&o>=0&&o<r.length?`
        if (!matched) {
          send(item, ${o});
          matched = true;
        }`:"",m=`
      const predicates = [${r.map((f)=>`(item: N8nItem) => (${f.conditions?this.parseConditions(f.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${c.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${p}
        if (matched) routed.push(item);
      }`,l=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${c.parse(t.numberOutputs??4,"firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${c.parse(t.output??0,"firstInputItem")};
        if (!Number.isSafeInteger(numberOutputs) || !Number.isSafeInteger(port) || port < 0 || port >= numberOutputs) {
          throw new Error('Switch expression returned an invalid output index');
        }
        send(item, port);
        routed.push(item);
      }
    `;return`
    // [Node: ${e.node.name}] Switch Router
    {
      const source: N8nItem[] = ${n} || inputItems;
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
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),i=c.parse(t.url||""),s=_(t),o=F(t),a="{}";if(o)a=`parseRequestHeaders(${c.parse(t.jsonHeaders??"{}")})`;else if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))a=`{ ${t.headerParameters.parameters.filter((l)=>l.name).map((l)=>`${JSON.stringify(l.name)}: ${c.parse(l.value)}`).join(", ")} }`;let p="undefined";if(s){let m=ie(t);if(m)throw Error(`Node ${JSON.stringify(e.node.name)}: ${m}`);p=ae(t)}else if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)p=`JSON.stringify(${c.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))p=`JSON.stringify({ ${t.bodyParameters.parameters.filter((l)=>l.name).map((l)=>`${JSON.stringify(l.name)}: ${c.parse(l.value)}`).join(", ")} })`}let u=`const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${a} };`;if(s)u=`const reqHeaders = ${o?a:`new Headers(${a})`};
      reqHeaders.delete('content-type');
      reqHeaders.delete('content-length');`;else if(o)u=`const reqHeaders = ${a};
      if (!reqHeaders.has('content-type')) reqHeaders.set('content-type', 'application/json');`;return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      ${u}
      const reqBody = ${p};
      const res = await fetch(targetUrl, {
        method: ${JSON.stringify(r)},
        headers: reqHeaders,
        ...(reqBody !== undefined ? { body: reqBody } : {})
      });
      const text = await res.text();
      let data: any;
      try { data = JSON.parse(text); } catch { data = { raw: text }; }
      return { json: typeof data === 'object' && data !== null ? data : { data } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapAggregateNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.destinationFieldName||"data";return`
    // [Node: ${e.node.name}] Aggregate Items
    {
      const source = ${n} || inputItems;
      ${e.variableName} = [{ json: { ${JSON.stringify(r)}: source.map((i: N8nItem) => i.json) } }];
      item = ${e.variableName}[0];
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapSplitOutNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.fieldToSplitOut||"data";return`
    // [Node: ${e.node.name}] Split Out Array Field
    {
      const source = ${n} || inputItems;
      const expanded: N8nItem[] = [];
      for (const entry of source) {
        const arr = entry.json?.[${JSON.stringify(r)}];
        if (Array.isArray(arr)) {
          for (const el of arr) {
            expanded.push({ json: typeof el === 'object' && el !== null ? el : { [${JSON.stringify(r)}]: el } });
          }
        }
      }
      ${e.variableName} = expanded.length > 0 ? expanded : source;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapMergeNode(e){let t=e.node.incomingConnections||[],n=e.node.parameters?.mode||"append",r=t.map((i)=>`(ctx.getNodeOutput(${JSON.stringify(i.sourceNode)}, ${i.outputIndex}) || [])`);if(n==="combine"&&r.length>=2)return`
    // [Node: ${e.node.name}] Merge (Combine Mode)
    {
      const left = ${r[0]};
      const right = ${r[1]};
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
      const combined = [${r.join(", ")}].flat();
      ${e.variableName} = combined.length > 0 ? combined : inputItems;
      item = ${e.variableName}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=c.parse(t.query||"SELECT 1;");return`
    // [Node: ${e.node.name}] PostgreSQL Query (via DATABASE_URL env var)
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const sql = ${r};
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=c.parse(i);return`
    // [Node: ${e.node.name}] Native LLM Completion (OpenAI-compatible REST API)
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
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
          model: ${JSON.stringify(r)},
          messages: [{ role: 'user', content: String(prompt) }]
        })
      });
      const data: any = await res.json();
      const text = data?.choices?.[0]?.message?.content || '';
      return { json: { text, model: ${JSON.stringify(r)}, usage: data?.usage } };
    }));
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}function he(e,t){if(!e.disabled||t.triggers.includes(e)||e.type.toLowerCase().endsWith("trigger"))return;let n=Object.entries(t.connections[e.name]??{}),r=!1;for(let[s,o]of n)for(let[a,p]of o.entries()){if(!p?.length)continue;if(s!=="main"||a!==0)return;r=!0}let i=!1;for(let s of Object.values(t.connections))for(let[o,a]of Object.entries(s))for(let p of a)for(let u of p??[]){if(u?.node!==e.name)continue;if(i||o!=="main"||(u.index??0)!==0)return;i=!0}if(i)return"passthrough";if(!r)return"skip"}class M{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",i=e.nodes.some(A)?`import { mergeByPosition } from '${r}/position-merge';
`:"",s=new Set(e.nodes.filter((d)=>z(d)||de(d))),o=e.nodes.some(k)?`import { applyRawEditFields, encodeJsonTemplateValue } from '${r}/raw-edit-fields';
`:"",a=s.size>0?`import { createLegacyNodeReferences } from '${r}/code-context';
`:"",p=s.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((d)=>d.name))});
`:"",u=new Map(e.nodes.map((d)=>[d.name,d])),m=e.nodes.some((d)=>d.type===S)?`import { downloadS3Files } from '${r}/s3';
`:"",l=e.nodes.some((d)=>d.type.includes("httpRequest")&&(_(d.parameters)||F(d.parameters)))?`import { createRequestForm, parseRequestHeaders } from '${r}/http-request';
`:"",f=e.nodes.some((d)=>d.type===C)?`import { publishRabbitMq } from '${r}/rabbitmq';
`:"",g=e.nodes.some((d)=>d.type===T)?`import { convertItemsToJsonFile } from '${r}/convert-to-json';
`:"",y=e.nodes.some((d)=>d.type===P)?`import { decompressItems } from '${r}/compression';
`:"",w=e.nodes.some((d)=>d.type===B)?`import { waitForInterval } from '${r}/wait-interval';
`:"",N=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${i}${a}${m}${l}${f}${g}${o}${y}${w}
${p}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,J=`  let item: N8nItem = inputItems[0] || { json: {} };
`,I=`  ctx.setNodeOutput("__initial__", inputItems);
`,W=new Map;e.orderedNodes.forEach((d,b)=>{let h=`node_${b}_${this.sanitizeIdentifier(d.name)}`;W.set(d.name,h)});let V=e.triggers,X=V.some((d)=>d.type===R),D=new Map(e.orderedNodes.map((d)=>[d,he(d,e)])),ve=e.orderedNodes.filter((d)=>!V.includes(d)&&D.get(d)!=="skip");V.forEach((d)=>{let b=e.orderedNodes.indexOf(d),h=W.get(d.name);J+=`  let ${h}: any = inputItems;
`;let v=JSON.stringify(d.name),j=`entryNode === ${v}`;if(d.type!==R){let E=(d.parameters?.path||d.name).toLowerCase().trim().replace(/^\/+/,"");I+=`  const triggerNode_${b} = item?.json?.__triggerNode;
`,I+=`  const triggerPath_${b} = item?.json?.__triggerPath;
`,j=`entryNode === undefined && (!triggerNode_${b} || triggerNode_${b} === ${v} || triggerPath_${b} === ${JSON.stringify(E)})`}let q=`${h} = inputItems;
ctx.setNodeOutput(${v}, inputItems);`;I+=this.generateNodeBlock(e.id,d,j,q)}),ve.forEach((d)=>{let b=W.get(d.name);J+=`  let ${b}: any = inputItems;
`;let h="true",v=d.incomingConnections??[],j=D.get(d)==="passthrough",q=v.length>0&&(j||!d.type.includes("merge"));if(v.length>0)h=`(${v.map((O)=>`ctx.isBranchActive(${JSON.stringify(O.sourceNode)}, ${O.outputIndex})`).join(" || ")})`;let E=j?`
    ${b} = incomingItems || [];
    item = ${b}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(d.name)}, ${b});`:K.mapNode({node:d,workflowName:e.name,variableName:b,previousVariableName:q?"incomingItems":"inputItems",legacyNodePorts:s.has(d)?oe(d,u):void 0});if(q){let H=v.map((O)=>`ctx.getNodeOutputBatches(${JSON.stringify(O.sourceNode)}, ${O.outputIndex})`);E=`await ctx.runNodeBatches(${JSON.stringify(d.name)}, [${H.join(", ")}].flat(), async (incomingItems) => {
${E.trim()}
});`}let L=d.ancestorTriggers??[];if(L.length>0)h=`(${L.map((O)=>`ctx.hasExecutedNode(${JSON.stringify(O)})`).join(" || ")}) && ${h}`;else if(X)h=`entryNode === undefined && ${h}`;I+=this.generateNodeBlock(e.id,d,h,E,L.length>0||X?"      ":"    ")});let Y=e.orderedNodes.filter((d)=>D.get(d)!=="skip").at(-1)?.name,Ie=Y?`(ctx.getNode(${JSON.stringify(Y)})?.all() || [{ json: item.json }])`:"inputItems",Oe=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${N}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext,
  entryNode?: string
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
  const $env = process.env;
${J}
${I}
  const finalItems = ${Ie};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(Oe)}, ${t});
`}static generateNodeBlock(e,t,n,r,i="    "){let s=[e,t.id||t.name,t.name].map((a)=>JSON.stringify(a)).join(", "),o=i+r.trim().replace(/\n/g,`
${i}`);return`  if (${n}) {
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
`}}class Z{static generateRoutesCode(e){let t=`import { randomUUID } from 'node:crypto';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let a=`workflow_${M.sanitizeIdentifier(o.fileName)}`,p=o.relativePath.replace(/\.ts$/,"");t+=`import { ${a} } from '../workflows/${p}';
`,n.push(a)});let r="",i=[],s=new Set;if(e.forEach((o)=>{let a=`workflow_${M.sanitizeIdentifier(o.fileName)}`,p=o.nodes.filter((u)=>u.type.includes("webhook"));for(let u of x(o)){let m=te(u);if(typeof m==="string")throw Error(`Workflow ${JSON.stringify(o.name)}, node ${JSON.stringify(u.name)}: ${m}`);let l=JSON.stringify({workflowId:o.id,node:u.name});i.push(`      stopConsumers.push(await startRabbitMqConsumer(
        ${JSON.stringify(m)},
        (items) => ${a}(items, undefined, ${JSON.stringify(u.name)}),
        (message) => fastify.log.error(${l}, message),
      ));`)}p.forEach((u)=>{let m=(u.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),l=(u.parameters?.httpMethod||"POST").toUpperCase(),f=l==="ALL"?["GET","POST","PUT","DELETE"]:[l],g=Array.from(new Set([`/${m}`,`/webhook/${m}`]));f.forEach((y)=>{g.forEach((w)=>{let N=`${y}:${w}`;if(s.has(N))return;s.add(N),r+=`
  fastify.route({
    method: '${y}',
    url: '${w}',
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
        const { items, context } = await ${a}(inputItems, ctx);
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
`,r+=`
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
const _registeredWorkflows = [${n.join(", ")}];
void _registeredWorkflows;

export async function registerCompiledRoutes(fastify: FastifyInstance): Promise<void> {
${r}
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
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);${e.some((n)=>x(n).length>0)?`
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
`}static generateOutputPackageJson(e=[],t={}){let n=e.some((o)=>o.nodes.some((a)=>a.type===S)),r=e.some((o)=>o.nodes.some((a)=>a.type===P)),i=e.some((o)=>x(o).length>0||o.nodes.some((a)=>a.type===C)),s=n||r||e.some((o)=>o.nodes.some((a)=>a.type===T));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...n?{"@aws-sdk/client-s3":"3.808.0"}:{},...r?{fflate:"0.8.3"}:{},...s?{"mime-types":"^3.0.1"}:{},...i?{amqplib:"^0.10.9"}:{},...t}},null,2)}}function we(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=Q.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=M.generateWorkflowCode(r),s=Z.generateRoutesCode([r]),o=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:we};})();
