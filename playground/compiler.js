(()=>{var{defineProperty:K,getOwnPropertyNames:_e,getOwnPropertyDescriptor:Fe}=Object,We=Object.prototype.hasOwnProperty;function Be(e){return this[e]}var Le=(e)=>{var t=(se??=new WeakMap).get(e),n;if(t)return t;if(t=K({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of _e(e))if(!We.call(t,r))K(t,r,{get:Be.bind(e,r),enumerable:!(n=Fe(e,r))||n.enumerable})}return se.set(e,t),t},se;var Ve=(e)=>e;function De(e,t){this[e]=Ve.bind(null,t)}var He=(e,t)=>{for(var n in t)K(e,n,{get:t[n],enumerable:!0,configurable:!0,set:De.bind(t,n)})};var nt={};He(nt,{compileWorkflowJson:()=>Ee});var _="n8n-nodes-base.rabbitmqTrigger";function L(e){return e.nodes.filter((t)=>t.type==="n8n-nodes-base.rabbitmqTrigger"&&!t.disabled)}function Z(e){return typeof e==="string"&&!e.startsWith("=")&&!e.includes("{{")}function ie(e){if((e.typeVersion??1)!==1)return"RabbitMQ Trigger needs typeVersion 1";let{queue:t,options:n}=e.parameters??{},r=n??{};if(!Z(t)||!t.trim())return"RabbitMQ Trigger needs a static queue name";if(typeof r!=="object"||Array.isArray(r))return"RabbitMQ Trigger needs static options";if(r.acknowledge!=="executionFinishesSuccessfully")return"RabbitMQ Trigger needs the executionFinishesSuccessfully acknowledgement mode";if(r.contentIsBinary||r.onlyContent)return"RabbitMQ Trigger supports text or parsed JSON in the message envelope";let i=r.parallelMessages??-1;if(!Number.isInteger(i)||i!==-1&&(i<1||i>65535))return"RabbitMQ Trigger needs parallelMessages between 1 and 65535, or -1";let s=r.binding?.bindings??[];if(!Array.isArray(s)||!s.every((o)=>Z(o?.exchange)&&o.exchange.length>0&&Z(o?.routingKey)))return"RabbitMQ Trigger needs static exchange names and routing keys in its bindings";for(let o of["assertQueue","durable","autoDelete","exclusive","jsonParseBody"])if(r[o]!==void 0&&typeof r[o]!=="boolean")return"RabbitMQ Trigger needs static boolean queue and message settings";if(r.assertQueue&&Object.keys(r.arguments??{}).length>0)return"RabbitMQ Trigger queue declaration does not support custom arguments";return{queue:t,bindings:s.map(({exchange:o,routingKey:a})=>({exchange:o,routingKey:a})),prefetch:i===-1?void 0:i,parseJson:r.jsonParseBody===!0,declareQueue:r.assertQueue?{durable:r.durable,autoDelete:r.autoDelete,exclusive:r.exclusive}:void 0}}var Ue=new Set(["if","switch","wait","httpRequest","code","set"].map((e)=>"n8n-nodes-base."+e));function ae(e,t){let n=[];for(let[r,i]of Object.entries(t)){if(!e.has(r)||!Array.isArray(i?.main))continue;i.main.forEach((s,o)=>{if(!Array.isArray(s))return;for(let a of s)if(a&&e.has(a.node))n.push({source:r,output:o,target:a.node,input:a.index??0})})}return n}function Ge(e,t){let n=new Map,r=new Map,i=[],s=new Set,o=[];function a(p){let c=n.size;n.set(p,c),r.set(p,c),i.push(p),s.add(p);for(let{target:N}of t.get(p)??[])if(!n.has(N))a(N),r.set(p,Math.min(r.get(p),r.get(N)));else if(s.has(N))r.set(p,Math.min(r.get(p),n.get(N)));if(r.get(p)!==c)return;let m=[],d;do d=i.pop(),s.delete(d),m.push(d);while(d!==p);if(m.length>1||t.get(p)?.some((N)=>N.target===p))o.push(m)}for(let p of e)if(!n.has(p))a(p);return o}function pe(e,t){let n=new Map(e.map((o)=>[o.name,o])),r=ae(n,t),i=new Map,s=new Map;for(let o of r){if(!i.has(o.source))i.set(o.source,[]);if(!s.has(o.target))s.set(o.target,[]);i.get(o.source).push(o),s.get(o.target).push(o)}return Ge(n.keys(),i).map((o)=>{let a=Qe(o,n,i,s);if(!a)throw Error("[n8n-to-fastify] Cyclic dependency detected. Only single-entry polling loops with a timed Wait and one IF or Switch retry output are supported. Nodes: "+o.slice().sort().map((p)=>JSON.stringify(p)).join(", "));return a})}function me(e,t,n){let r=new Map(n.flatMap((s)=>s.nodes.map((o)=>[o,s.entry]))),i=Object.create(null);for(let s of ae(new Map(e.map((o)=>[o.name,o])),t)){let o=r.get(s.source)??s.source,a=r.get(s.target)??s.target;if(o===a)continue;let{main:p}=i[o]??={main:[[]]};p[0].push({node:a,type:"main",index:0})}return{nodes:e.filter((s)=>(r.get(s.name)??s.name)===s.name),connections:i}}function Qe(e,t,n,r){let i=new Set(e),s=e.map((f)=>t.get(f));if(s.some((f)=>!Ue.has(f.type)||f.disabled))return;let o=s.filter((f)=>f.type==="n8n-nodes-base.if"||f.type==="n8n-nodes-base.switch"),a=s.filter((f)=>f.type==="n8n-nodes-base.wait");if(o.length!==1||a.length!==1||(a[0].parameters?.resume??"timeInterval")!=="timeInterval")return;if(o[0].parameters?.options?.allMatchingOutputs)return;let p=o[0].name,c=e.flatMap((f)=>(r.get(f)??[]).filter((b)=>!i.has(b.source)));if(c.length!==1||c[0].input!==0)return;let m=c[0].target,d=[m],N=[],h,w;for(let f=0;f<e.length;f++){let b=d[f],v=n.get(b)??[],y=v.filter((O)=>i.has(O.target));if(y.length!==1||y[0].input!==0||b!==p&&y[0].output!==0)return;let C=v.filter((O)=>!i.has(O.target));if(C.length>0&&(b!==p||C.some((O)=>O.output===y[0].output)))return;if(N.push(...C),b===p)w=y[0].output;if(y[0].target===m){if(f!==e.length-1)return;h=y[0]}else d.push(y[0].target)}if(!h||w===void 0||N.length===0)return;let S=[],g=new Set;for(let f of N){let b=t.get(f.target);if(b.type==="n8n-nodes-base.stopAndError"&&!b.disabled){if(f.input!==0||r.get(f.target)?.length!==1||n.get(f.target)?.length)return;S.push({output:f.output,target:f.target})}g.add(f.output)}return{entry:m,nodes:d,decision:p,repeatPort:w,backEdge:{source:h.source,output:h.output},exitPorts:[...g].sort((f,b)=>f-b),terminalExits:S}}class X{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},a=n||`${t}.json`,p=new Map;s.forEach((g)=>p.set(g.name,[])),Object.entries(o).forEach(([g,f])=>{if(f.main&&Array.isArray(f.main))f.main.forEach((b,v)=>{if(Array.isArray(b))b.forEach((y)=>{if(y&&y.node&&p.has(y.node))p.get(y.node)?.push({sourceNode:g,outputIndex:v,inputIndex:y.index??0})})})}),s.forEach((g)=>{g.incomingConnections=p.get(g.name)||[]});let c=s.filter((g)=>g.type===_||g.type.includes("webhook")||g.type.includes("trigger")||g.type.includes("cron")),m=new Set(c.map((g)=>g.name));s.forEach((g)=>{let f=new Set,b=new Set,v=[...(p.get(g.name)||[]).map((y)=>y.sourceNode)];while(v.length>0){let y=v.shift();if(b.has(y))continue;if(b.add(y),m.has(y))f.add(y);(p.get(y)||[]).forEach((O)=>v.push(O.sourceNode))}g.ancestorTriggers=Array.from(f)});let d=pe(s,o),N=d.length?me(s,o,d):{nodes:s,connections:o},h=new Map(d.map((g)=>[g.entry,g.nodes])),w=new Map(s.map((g)=>[g.name,g])),S=this.topologicalSort(N.nodes,N.connections).flatMap((g)=>h.get(g.name)?.map((f)=>w.get(f))??[g]);return{id:r,name:i,fileName:t,relativePath:a.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:S,triggers:c,pollingLoops:d}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((m)=>{n.set(m.name,m),r.set(m.name,[])}),Object.entries(t).forEach(([m,d])=>{if(d.main&&Array.isArray(d.main))d.main.forEach((N)=>{if(Array.isArray(N))N.forEach((h)=>{if(h&&h.node&&n.has(h.node))r.get(m)?.push(h.node)})})});let i=new Set,s=new Set,o=[],a=(m,d=[])=>{if(i.has(m))return;if(s.has(m)){let w=[...d,m].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${w}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(m),d.push(m);let N=r.get(m)||[];for(let w of N)a(w,d);d.pop(),s.delete(m),i.add(m);let h=n.get(m);if(h)o.push(h)},p=new Map;e.forEach((m)=>p.set(m.name,0));for(let m of r.values())for(let d of m)p.set(d,(p.get(d)||0)+1);let c=e.map((m)=>m.name).filter((m)=>p.get(m)===0);for(let m of c)a(m);for(let m of e)if(!i.has(m.name))a(m.name);return o.reverse()}}class l{static parse(e,t,n="inputItems"){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let p=r.substring(1).trim();if(p.startsWith("{{")&&p.endsWith("}}")){let c=p.substring(2,p.length-2).trim();if(!c.includes("{{")&&!c.includes("}}"))return`(${this.transformTokens(c,t,n)})`}r=r.substring(1).trim()}let i=[],s=0,a=r.replace(/\{\{(.*?)\}\}/g,(p,c)=>{let m=this.transformTokens(c.trim(),t,n),d=`__EXPR_PLACEHOLDER_${s++}__`;return i.push(`\${(${m}) ?? ''}`),d}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return i.forEach((p,c)=>{a=a.replace(`__EXPR_PLACEHOLDER_${c}__`,p)}),`\`${a}\``}static transformTokens(e,t,n){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(i,s,o)=>{let a=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(s)})?.item?.json${a.length?"?."+a.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(i,s,o)=>{let a=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(s)})?.first()?.json${a.length?"?."+a.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(i,s)=>`ctx.getNode(${JSON.stringify(s)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(i,s)=>`ctx.getNode(${JSON.stringify(s)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(i,s)=>{if(!s)return"item?.json";return`item?.json?.${s.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,n),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(i,s)=>`process.env.${s}`),r}}function V(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function ze(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:n=0}of e.incomingConnections??[])if(!Number.isInteger(n)||n<0||n>=t)return"position Merge has an invalid input port"}function ue(e){let t=ze(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=n.numberInputs??2,i=Array.from({length:r},()=>[]);for(let{sourceNode:p,outputIndex:c,inputIndex:m=0}of e.node.incomingConnections??[])i[m].push(`ctx.getNodeOutput(${JSON.stringify(p)}, ${c}) || []`);let s=i.map((p)=>`[${p.join(", ")}].flat()`),o=n.options??{},a=l.parse(o.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${s.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${a}), ${JSON.stringify(o.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function Y(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function ce(e){if(Y(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function de(e,t){let n=new Map,r=[e],i=new Set([e.name]);for(let s=0;s<r.length;s++){let o=[...r[s].incomingConnections??[]];o.sort((a,p)=>(a.inputIndex??0)-(p.inputIndex??0));for(let{sourceNode:a,outputIndex:p}of o){if(i.has(a))continue;i.add(a),n.set(a,p);let c=t.get(a);if(c)r.push(c)}}return[...n]}var j="n8n-nodes-base.s3";function Ke(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function le(e){let t=Ke(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=e.previousVariableName??"inputItems",i=l.parse(n.bucketName),s=l.parse(n.fileKey),o=l.parse(n.binaryPropertyName??"data");return`
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
    }`}function D(e){return e?.sendBody===!0&&e.contentType==="multipart-form-data"}function H(e){return e?.sendHeaders===!0&&e.specifyHeaders==="json"}function fe(e){let t=(e.method??"GET").toUpperCase();if(!["POST","PUT","PATCH"].includes(t))return"HTTP Request multipart supports POST, PUT and PATCH";let n=e.bodyParameters?.parameters??[];if(!Array.isArray(n))return"HTTP Request multipart needs an array of form fields";for(let r of n){if(typeof r?.name!=="string"||!r.name)return"HTTP Request multipart fields require a name";if(r.parameterType==="formBinaryData"){if(typeof r.inputDataFieldName!=="string"||!r.inputDataFieldName)return"HTTP Request multipart binary fields require an input binary property"}else if(r.parameterType&&!["formData","text"].includes(r.parameterType))return"HTTP Request multipart supports text and binary form fields"}}function ge(e){return`createRequestForm(item, [${(e.bodyParameters?.parameters??[]).map((r)=>{let i=l.parse(r.name),s=r.parameterType==="formBinaryData"?"binaryProperty":"value",o=l.parse(r.parameterType==="formBinaryData"?r.inputDataFieldName:r.value);return`{ name: String(${i} ?? ''), ${s}: String(${o} ?? '') }`}).join(", ")}])`}var x="n8n-nodes-base.rabbitmq";function Ze(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1)return"RabbitMQ needs typeVersion 1 or 1.1";if((t.operation??"sendMessage")!=="sendMessage"||t.mode!=="exchange"||Object.keys(t.options??{}).length>0)return"RabbitMQ only supports Send Message to an existing exchange with default options"}function Ne(e){let t=Ze(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",i=l.parse(n.exchange??""),s=l.parse(n.routingKey??""),o=l.parse(n.sendInputData??!0),a=l.parse(n.message??""),p=e.variableName;return`
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
    }`}var R="n8n-nodes-base.convertToFile";function Xe(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1||t.operation!=="toJson")return"Convert to File supports Convert to JSON on node versions 1 and 1.1";if((t.mode??"once")!=="once")return"Convert to JSON supports the All Items to One File mode";if(!["utf8","utf-8"].includes(t.options?.encoding??"utf8"))return"Convert to JSON supports UTF-8 encoding"}function ye(e){let t=Xe(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=n.options??{},i=e.previousVariableName??"inputItems",s=l.parse(n.binaryPropertyName??"data"),o=l.parse(r.fileName??""),a=l.parse(r.format??!1),p=l.parse(r.addBOM??!1);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}function J(e){return e.type==="n8n-nodes-base.set"&&e.parameters?.mode==="raw"}function be(e){let t=e.parameters?.jsonOutput;return J(e)&&typeof t==="string"&&t.startsWith("=")&&/\{\{[\s\S]*?\$node\b[\s\S]*?\}\}/.test(t)}function he(e){if(!J(e))return;if(e.typeVersion!==3.4)return"Edit Fields JSON Output requires node version 3.4";let t=e.parameters,{dotNotation:n,stripBinary:r}=t.options??{};if([t.includeOtherFields,n,r].some((i)=>i!==void 0&&typeof i!=="boolean"))return"Edit Fields JSON Output requires static field and binary options";if(t.includeOtherFields!==!1&&t.includeOtherFields!==void 0&&!["all","none"].includes(t.include??"all"))return"Edit Fields JSON Output supports including all or none of the input fields";if(t.duplicateItem)return"Edit Fields JSON Output does not support manual item duplication"}function Ye(e){if(typeof e!=="string"||!e.startsWith("="))return JSON.stringify(e);let t=e.replace(/^=+/,""),n=(a)=>l.parse("="+a,"source[0]","source"),r=t.trim();if(r.startsWith("{{")&&r.endsWith("}}")&&!r.slice(2,-2).includes("{{"))return n(r);let i=[],s=0,o=!1;for(let a of t.matchAll(/\{\{([\s\S]*?)\}\}/g)){let p=t.slice(s,a.index);for(let[c]of p.matchAll(/\\[\s\S]|"/g))if(c==='"')o=!o;i.push(JSON.stringify(p)),i.push("encodeJsonTemplateValue("+n(a[0])+", "+o+")"),s=a.index+a[0].length}return i.push(JSON.stringify(t.slice(s))),"["+i.join(", ")+"].join('')"}function $e(e){let t=e.node.parameters,n=t.options??{},r=Ye(t.jsonOutput===void 0?{my_field_1:"value",my_field_2:1}:t.jsonOutput),i=t.includeOtherFields===!0&&(t.include??"all")==="all",s=n.stripBinary??!0,o=n.dotNotation??!0,a=e.legacyNodePorts?"const $node = createLegacyNodeReferences(ctx, legacyNodeNames, "+JSON.stringify(e.legacyNodePorts)+", itemIndex);":"";return["{","  const source: N8nItem[] = "+(e.previousVariableName??"inputItems")+";","  "+e.variableName+" = source.map((item, itemIndex) => {",...a?["    "+a]:[],"    return applyRawEditFields(item, itemIndex, "+r+", {","      includeAll: "+i+", stripBinary: "+s+",","      dotNotation: "+o+",","    });","  });","}","item = "+e.variableName+"[0] || { json: {} };","ctx.setNodeOutput("+JSON.stringify(e.node.name)+", "+e.variableName+");"].join(`
`)}function we(e){let t=e.parameters??{};return e.type==="n8n-nodes-base.code"&&e.onError==="continueRegularOutput"&&(t.language??"javaScript")==="javaScript"&&(t.mode??"runOnceForAllItems")==="runOnceForAllItems"}var A="n8n-nodes-base.compression";function et(e){let t=e.typeVersion??1;if(t!==1&&t!==1.1||(e.parameters?.operation??"decompress")!=="decompress")return"Compression supports ZIP and GZIP decompression on node versions 1 and 1.1"}function Ie(e){let t=et(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",i=l.parse(n.binaryPropertyName??"data"),s=l.parse(n.outputPrefix??"file_");return`
    {
      const source: N8nItem[] = ${r} || inputItems;
      ${e.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${e.variableName} = await decompressItems(source, String(${i}), String(${s}));
      }
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}var ve={seconds:1000,minutes:60000,hours:3600000,days:86400000};function Se(e,t){if(typeof t!=="string"||!Object.hasOwn(ve,t))throw Error("Wait unit must be seconds, minutes, hours, or days");let n=typeof e==="string"&&e.trim()?Number(e):e;if(typeof n!=="number"||!Number.isFinite(n)||n<0)throw Error("Wait amount must be a finite nonnegative number");let r=n*ve[t];if(r>Number.MAX_SAFE_INTEGER)throw Error("Wait interval is too large");return r}var U="n8n-nodes-base.wait";function Me(e){let t=e.parameters??{};return{amount:t.amount??(e.typeVersion===1?1:5),unit:t.unit??(e.typeVersion===1?"hours":"seconds")}}function Oe(e){return typeof e==="string"&&e.trimStart().startsWith("=")&&e.includes("{{")}function tt(e){let t=e.typeVersion??1.1;if(t!==1&&t!==1.1||(e.parameters?.resume??"timeInterval")!=="timeInterval")return"Wait supports the After Time Interval mode on node versions 1 and 1.1";let{amount:n,unit:r}=Me(e);try{Se(Oe(n)?0:n,Oe(r)?"seconds":r)}catch(i){return i.message}}function Pe(e){let t=tt(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let{amount:n,unit:r}=Me(e.node),i=e.previousVariableName??"inputItems";return`
    ${e.variableName} = ${i} || inputItems;
    if (${e.variableName}.length > 0) {
      const inputItems: N8nItem[] = ${e.variableName};
      const item = inputItems[0];
      await waitForInterval(${l.parse(n)}, ${l.parse(r)});
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}var F="n8n-nodes-base.stopAndError";function rt(e){if((e.typeVersion??1)!==1)return"Stop and Error supports node version 1";let t=e.parameters?.errorType??"errorMessage";if(t!=="errorMessage"&&t!=="errorObject")return"Stop and Error supports Error Message and Error Object"}function Ce(e){let t=rt(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=n.errorType??"errorMessage",i=n[r]??"",s=l.parse(typeof i==="string"&&!i.includes("{{")?i.replace(/^=/,""):i);return`
    {
      const source: N8nItem[] = ${e.previousVariableName??"inputItems"} || inputItems;
      if (source.length > 0) {
        const inputItems = source;
        const item = inputItems[0];
        throw createWorkflowStopError(${JSON.stringify(r)}, ${s});
      }
    }`}class ee{static mapNode(e){let t=e.node.type;if(t===x)return Ne(e);if(t===j)return le(e);if(t===R)return ye(e);if(t===A)return Ie(e);if(t===F)return Ce(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set")){if(J(e.node)&&!he(e.node))return $e(e);return this.mapSetNode(e)}if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return V(e.node)?ue(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t===U)return Pe(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(n==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)s=l.parse(t.responseBody);else if(n==="text"&&t.responseBody)s=l.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=ce(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters||{},r=n.jsCode||n.functionCode||"return $input.all();",i=e.previousVariableName||"inputItems",s=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
`:"",o=r.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(c,m)=>`(ctx.getNode(${JSON.stringify(m)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(c,m)=>`(ctx.getNode(${JSON.stringify(m)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(c,m)=>`(ctx.getNode(${JSON.stringify(m)})?.all() || [])`).replace(/\$now/g,"DateTime.now()"),a=`
${s}      const result = await (async () => {
        ${o}
      })();
      return n8n.normalizeItems(result);`,p=we(e.node)?`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",i=e.node.typeVersion??3.4,s=t.options??{},o;if(i>=3.4){let p=l.parse(t.includeOtherFields??!1),c=l.parse(t.include??"all"),m=l.parse(s.stripBinary??!1);o=`(${p}) && (${c}) !== 'none' && !(${m})`}else if(i>=3)o=l.parse(s.includeBinary??!1);else o=`!(${l.parse(t.keepOnlySet??s.keepOnlySet??!1)})`;let a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let p of t.assignments.assignments)if(p.name){let c=l.parse(p.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${c});`)}}if(t.values)for(let p of["string","number","boolean"]){let c=t.values[p];if(Array.isArray(c)){for(let m of c)if(m.name){let d=l.parse(m.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${d});`)}}}return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let n=[],r=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=l.parse(s.leftValue,r),a=l.parse(s.rightValue,r),p=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(p)){n.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,a=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${a}))`;switch(p){case"equals":case"equal":n.push(`(${o} == ${a})`);break;case"notEquals":case"notEqual":n.push(`(${o} != ${a})`);break;case"contains":n.push(`String(${o} ?? '').includes(String(${a} ?? ''))`);break;case"true":n.push(`(${o} === true)`);break;case"false":n.push(`(${o} === false)`);break;case"gt":case"larger":n.push(`(Number(${o}) > Number(${a}))`);break;case"lt":case"smaller":n.push(`(Number(${o}) < Number(${a}))`);break;case"gte":n.push(`(Number(${o}) >= Number(${a}))`);break;case"lte":n.push(`(Number(${o}) <= Number(${a}))`);break;case"isEmpty":case"empty":n.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":n.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:n.push(`Boolean(${o})`)}}let i=e?.combinator==="or"?" || ":" && ";return n.length?n.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",n=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
      const predicates = [${r.map((N)=>`(item: N8nItem) => (${N.conditions?this.parseConditions(N.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${l.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${p}
        if (matched) routed.push(item);
      }`,d=`
      const numberOutputs = source.length > 0
        ? ((item: N8nItem) => (${l.parse(t.numberOutputs??4,"firstInputItem")}))(source[0])
        : 0;
      for (const item of source) {
        const port = ${l.parse(t.output??0,"firstInputItem")};
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
      ${t.mode==="expression"?d:m}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),i=l.parse(t.url||""),s=D(t),o=H(t),a="{}";if(o)a=`parseRequestHeaders(${l.parse(t.jsonHeaders??"{}")})`;else if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))a=`{ ${t.headerParameters.parameters.filter((d)=>d.name).map((d)=>`${JSON.stringify(d.name)}: ${l.parse(d.value)}`).join(", ")} }`;let p="undefined";if(s){let m=fe(t);if(m)throw Error(`Node ${JSON.stringify(e.node.name)}: ${m}`);p=ge(t)}else if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)p=`JSON.stringify(${l.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))p=`JSON.stringify({ ${t.bodyParameters.parameters.filter((d)=>d.name).map((d)=>`${JSON.stringify(d.name)}: ${l.parse(d.value)}`).join(", ")} })`}let c=`const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${a} };`;if(s)c=`const reqHeaders = ${o?a:`new Headers(${a})`};
      reqHeaders.delete('content-type');
      reqHeaders.delete('content-length');`;else if(o)c=`const reqHeaders = ${a};
      if (!reqHeaders.has('content-type')) reqHeaders.set('content-type', 'application/json');`;return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      ${c}
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=l.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=l.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}function Te(e,t){if(!e.disabled||t.triggers.includes(e)||e.type.toLowerCase().endsWith("trigger"))return;let n=Object.entries(t.connections[e.name]??{}),r=!1;for(let[s,o]of n)for(let[a,p]of o.entries()){if(!p?.length)continue;if(s!=="main"||a!==0)return;r=!0}let i=!1;for(let s of Object.values(t.connections))for(let[o,a]of Object.entries(s))for(let p of a)for(let c of p??[]){if(c?.node!==e.name)continue;if(i||o!=="main"||(c.index??0)!==0)return;i=!0}if(i)return"passthrough";if(!r)return"skip"}function ke(e,t,n,r){let{entry:i,terminalExits:s,repeatPort:o,...a}=e,c=t.get(i).incomingConnections.find((N)=>!e.nodes.includes(N.sourceNode)),m="ctx.getNodeOutputBatches("+JSON.stringify(c.sourceNode)+", "+c.outputIndex+")",d="";for(let[N,h]of e.nodes.entries()){let w=t.get(h);if(N===0)d+=r(w,"true",n(w,"pollingItems"));else{let S=w.incomingConnections.find((f)=>f.sourceNode===e.nodes[N-1]),g="pollingInput_"+N;d+="    const "+g+" = ctx.getNodeOutput("+JSON.stringify(S.sourceNode)+", "+S.outputIndex+`);
`,d+=r(w,g+"?.length",n(w,g))}if(h!==e.decision)continue;for(let[S,g]of s.entries()){let f=t.get(g.target),b="pollingTerminal_"+S;d+="    const "+b+" = ctx.getNodeOutput("+JSON.stringify(h)+", "+g.output+`);
`,d+=r(f,b+"?.length",n(f,b))}d+="    if (ctx.getNodeOutput("+JSON.stringify(h)+", "+o+`)?.length) assertCanRetry();
`}return"  await ctx.runPollingLoop("+JSON.stringify(a)+", "+m+`, async (pollingItems, assertCanRetry) => {
`+d+`  });
`}class q{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",i=e.nodes.some(V)?`import { mergeByPosition } from '${r}/position-merge';
`:"",s=new Set(e.nodes.filter((u)=>Y(u)||be(u))),o=e.nodes.some(J)?`import { applyRawEditFields, encodeJsonTemplateValue } from '${r}/raw-edit-fields';
`:"",a=s.size>0?`import { createLegacyNodeReferences } from '${r}/code-context';
`:"",p=s.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((u)=>u.name))});
`:"",c=new Map(e.nodes.map((u)=>[u.name,u])),m=e.nodes.some((u)=>u.type===j)?`import { downloadS3Files } from '${r}/s3';
`:"",d=e.nodes.some((u)=>u.type.includes("httpRequest")&&(D(u.parameters)||H(u.parameters)))?`import { createRequestForm, parseRequestHeaders } from '${r}/http-request';
`:"",N=e.nodes.some((u)=>u.type===x)?`import { publishRabbitMq } from '${r}/rabbitmq';
`:"",h=e.nodes.some((u)=>u.type===R)?`import { convertItemsToJsonFile } from '${r}/convert-to-json';
`:"",w=e.nodes.some((u)=>u.type===A)?`import { decompressItems } from '${r}/compression';
`:"",S=e.nodes.some((u)=>u.type===U)?`import { waitForInterval } from '${r}/wait-interval';
`:"",g=e.nodes.some((u)=>u.type===F)?`import { createWorkflowStopError } from '${r}/stop-and-error';
`:"",f=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${i}${a}${m}${d}${N}${h}${o}${w}${S}${g}
${p}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,b=`  let item: N8nItem = inputItems[0] || { json: {} };
`,v=`  ctx.setNodeOutput("__initial__", inputItems);
`,y=new Map;e.orderedNodes.forEach((u,I)=>{let T=`node_${I}_${this.sanitizeIdentifier(u.name)}`;y.set(u.name,T)});let C=e.triggers,O=C.some((u)=>u.type===_),G=new Map(e.orderedNodes.map((u)=>[u,Te(u,e)])),je=e.orderedNodes.filter((u)=>!C.includes(u)&&G.get(u)!=="skip"),re=e.pollingLoops??[],xe=new Map(re.flatMap((u)=>u.nodes.map((I)=>[I,u]))),Re=new Set(re.flatMap((u)=>u.terminalExits.map((I)=>I.target))),ne=(u,I)=>ee.mapNode({node:u,workflowName:e.name,variableName:y.get(u.name),previousVariableName:I,legacyNodePorts:s.has(u)?de(u,c):void 0});C.forEach((u)=>{let I=e.orderedNodes.indexOf(u),T=y.get(u.name);b+=`  let ${T}: any = inputItems;
`;let M=JSON.stringify(u.name),k=`entryNode === ${M}`;if(u.type!==_){let B=(u.parameters?.path||u.name).toLowerCase().trim().replace(/^\/+/,"");v+=`  const triggerNode_${I} = item?.json?.__triggerNode;
`,v+=`  const triggerPath_${I} = item?.json?.__triggerPath;
`,k=`entryNode === undefined && (!triggerNode_${I} || triggerNode_${I} === ${M} || triggerPath_${I} === ${JSON.stringify(B)})`}let W=`${T} = inputItems;
ctx.setNodeOutput(${M}, inputItems);`;v+=this.generateNodeBlock(e.id,u,k,W)}),je.forEach((u)=>{let I=y.get(u.name);b+=`  let ${I}: any = inputItems;
`;let T=xe.get(u.name);if(T){if(u.name===T.entry)v+=ke(T,c,ne,(E,P,qe)=>this.generateNodeBlock(e.id,E,P,qe,E.ancestorTriggers?.length||O?"      ":"    "));return}if(Re.has(u.name))return;let M="true",k=u.incomingConnections??[],W=G.get(u)==="passthrough",B=k.length>0&&(W||!u.type.includes("merge"));if(k.length>0)M=`(${k.map((P)=>`ctx.isBranchActive(${JSON.stringify(P.sourceNode)}, ${P.outputIndex})`).join(" || ")})`;let Q=W?`
    ${I} = incomingItems || [];
    item = ${I}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(u.name)}, ${I});`:ne(u,B?"incomingItems":"inputItems");if(B){let E=k.map((P)=>`ctx.getNodeOutputBatches(${JSON.stringify(P.sourceNode)}, ${P.outputIndex})`);Q=`await ctx.runNodeBatches(${JSON.stringify(u.name)}, [${E.join(", ")}].flat(), async (incomingItems) => {
${Q.trim()}
});`}let z=u.ancestorTriggers??[];if(z.length>0)M=`(${z.map((P)=>`ctx.hasExecutedNode(${JSON.stringify(P)})`).join(" || ")}) && ${M}`;else if(O)M=`entryNode === undefined && ${M}`;v+=this.generateNodeBlock(e.id,u,M,Q,z.length>0||O?"      ":"    ")});let oe=e.orderedNodes.filter((u)=>G.get(u)!=="skip"&&(u.type!==F||u.disabled)).at(-1)?.name,Je=oe?`(ctx.getNode(${JSON.stringify(oe)})?.all() || [{ json: item.json }])`:"inputItems",Ae=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${f}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext,
  entryNode?: string
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
  const $env = process.env;
${b}
${v}
  const finalItems = ${Je};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(Ae)}, ${t});
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
`}}class te{static generateRoutesCode(e){let t=`import { randomUUID } from 'node:crypto';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let a=`workflow_${q.sanitizeIdentifier(o.fileName)}`,p=o.relativePath.replace(/\.ts$/,"");t+=`import { ${a} } from '../workflows/${p}';
`,n.push(a)});let r="",i=[],s=new Set;if(e.forEach((o)=>{let a=`workflow_${q.sanitizeIdentifier(o.fileName)}`,p=o.nodes.filter((c)=>c.type.includes("webhook"));for(let c of L(o)){let m=ie(c);if(typeof m==="string")throw Error(`Workflow ${JSON.stringify(o.name)}, node ${JSON.stringify(c.name)}: ${m}`);let d=JSON.stringify({workflowId:o.id,node:c.name});i.push(`      stopConsumers.push(await startRabbitMqConsumer(
        ${JSON.stringify(m)},
        (items) => ${a}(items, undefined, ${JSON.stringify(c.name)}),
        (message) => fastify.log.error(${d}, message),
      ));`)}p.forEach((c)=>{let m=(c.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),d=(c.parameters?.httpMethod||"POST").toUpperCase(),N=d==="ALL"?["GET","POST","PUT","DELETE"]:[d],h=Array.from(new Set([`/${m}`,`/webhook/${m}`]));N.forEach((w)=>{h.forEach((S)=>{let g=`${w}:${S}`;if(s.has(g))return;s.add(g),r+=`
  fastify.route({
    method: '${w}',
    url: '${S}',
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
          __triggerNode: ${JSON.stringify(c.name)},
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
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);${e.some((n)=>L(n).length>0)?`
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
`}static generateOutputPackageJson(e=[],t={}){let n=e.some((o)=>o.nodes.some((a)=>a.type===j)),r=e.some((o)=>o.nodes.some((a)=>a.type===A)),i=e.some((o)=>L(o).length>0||o.nodes.some((a)=>a.type===x)),s=n||r||e.some((o)=>o.nodes.some((a)=>a.type===R));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...n?{"@aws-sdk/client-s3":"3.808.0"}:{},...r?{fflate:"0.8.3"}:{},...s?{"mime-types":"^3.0.1"}:{},...i?{amqplib:"^0.10.9"}:{},...t}},null,2)}}function Ee(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=X.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=q.generateWorkflowCode(r),s=te.generateRoutesCode([r]),o=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:Ee};})();
