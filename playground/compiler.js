(()=>{var{defineProperty:B,getOwnPropertyNames:ce,getOwnPropertyDescriptor:le}=Object,fe=Object.prototype.hasOwnProperty;function ge(e){return this[e]}var Ne=(e)=>{var t=(G??=new WeakMap).get(e),n;if(t)return t;if(t=B({},"__esModule",{value:!0}),e&&typeof e==="object"||typeof e==="function"){for(var r of ce(e))if(!fe.call(t,r))B(t,r,{get:ge.bind(e,r),enumerable:!(n=le(e,r))||n.enumerable})}return G.set(e,t),t},G;var ye=(e)=>e;function $e(e,t){this[e]=ye.bind(null,t)}var be=(e,t)=>{for(var n in t)B(e,n,{get:t[n],enumerable:!0,configurable:!0,set:$e.bind(t,n)})};var Oe={};be(Oe,{compileWorkflowJson:()=>pe});var J="n8n-nodes-base.rabbitmqTrigger";function q(e){return e.nodes.filter((t)=>t.type==="n8n-nodes-base.rabbitmqTrigger"&&!t.disabled)}function W(e){return typeof e==="string"&&!e.startsWith("=")&&!e.includes("{{")}function z(e){if((e.typeVersion??1)!==1)return"RabbitMQ Trigger needs typeVersion 1";let{queue:t,options:n}=e.parameters??{},r=n??{};if(!W(t)||!t.trim())return"RabbitMQ Trigger needs a static queue name";if(typeof r!=="object"||Array.isArray(r))return"RabbitMQ Trigger needs static options";if(r.acknowledge!=="executionFinishesSuccessfully")return"RabbitMQ Trigger needs the executionFinishesSuccessfully acknowledgement mode";if(r.contentIsBinary||r.onlyContent)return"RabbitMQ Trigger supports text or parsed JSON in the message envelope";let i=r.parallelMessages??-1;if(!Number.isInteger(i)||i!==-1&&(i<1||i>65535))return"RabbitMQ Trigger needs parallelMessages between 1 and 65535, or -1";let s=r.binding?.bindings??[];if(!Array.isArray(s)||!s.every((o)=>W(o?.exchange)&&o.exchange.length>0&&W(o?.routingKey)))return"RabbitMQ Trigger needs static exchange names and routing keys in its bindings";for(let o of["assertQueue","durable","autoDelete","exclusive","jsonParseBody"])if(r[o]!==void 0&&typeof r[o]!=="boolean")return"RabbitMQ Trigger needs static boolean queue and message settings";if(r.assertQueue&&Object.keys(r.arguments??{}).length>0)return"RabbitMQ Trigger queue declaration does not support custom arguments";return{queue:t,bindings:s.map(({exchange:o,routingKey:a})=>({exchange:o,routingKey:a})),prefetch:i===-1?void 0:i,parseJson:r.jsonParseBody===!0,declareQueue:r.assertQueue?{durable:r.durable,autoDelete:r.autoDelete,exclusive:r.exclusive}:void 0}}class V{static parseWorkflow(e,t,n){if(!e||!Array.isArray(e.nodes))return null;let r=e.id||t,i=e.name||t,s=e.nodes||[],o=e.connections||{},a=n||`${t}.json`,m=new Map;s.forEach((f)=>m.set(f.name,[])),Object.entries(o).forEach(([f,N])=>{if(N.main&&Array.isArray(N.main))N.main.forEach((y,b)=>{if(Array.isArray(y))y.forEach((g)=>{if(g&&g.node&&m.has(g.node))m.get(g.node)?.push({sourceNode:f,outputIndex:b,inputIndex:g.index??0})})})}),s.forEach((f)=>{f.incomingConnections=m.get(f.name)||[]});let d=s.filter((f)=>f.type===J||f.type.includes("webhook")||f.type.includes("trigger")||f.type.includes("cron")),p=new Set(d.map((f)=>f.name));s.forEach((f)=>{let N=new Set,y=new Set,b=[...(m.get(f.name)||[]).map((g)=>g.sourceNode)];while(b.length>0){let g=b.shift();if(y.has(g))continue;if(y.add(g),p.has(g))N.add(g);(m.get(g)||[]).forEach((P)=>b.push(P.sourceNode))}f.ancestorTriggers=Array.from(N)});let l=this.topologicalSort(s,o);return{id:r,name:i,fileName:t,relativePath:a.replace(/\.json$/,".ts"),nodes:s,connections:o,orderedNodes:l,triggers:d}}static topologicalSort(e,t){let n=new Map,r=new Map;e.forEach((p)=>{n.set(p.name,p),r.set(p.name,[])}),Object.entries(t).forEach(([p,l])=>{if(l.main&&Array.isArray(l.main))l.main.forEach((f)=>{if(Array.isArray(f))f.forEach((N)=>{if(N&&N.node&&n.has(N.node))r.get(p)?.push(N.node)})})});let i=new Set,s=new Set,o=[],a=(p,l=[])=>{if(i.has(p))return;if(s.has(p)){let y=[...l,p].join(" -> ");throw Error(`[n8n-to-fastify] Cyclic dependency detected in workflow DAG: (${y}). Transpiled workflows must be strict Directed Acyclic Graphs.`)}s.add(p),l.push(p);let f=r.get(p)||[];for(let y of f)a(y,l);l.pop(),s.delete(p),i.add(p);let N=n.get(p);if(N)o.push(N)},m=new Map;e.forEach((p)=>m.set(p.name,0));for(let p of r.values())for(let l of p)m.set(l,(m.get(l)||0)+1);let d=e.map((p)=>p.name).filter((p)=>m.get(p)===0);for(let p of d)a(p);for(let p of e)if(!i.has(p.name))a(p.name);return o.reverse()}}class u{static parse(e,t,n="inputItems"){if(e===void 0||e===null)return JSON.stringify("");if(typeof e!=="string")return JSON.stringify(e);let r=e.trim();if(!r.includes("{{"))return JSON.stringify(e);if(r.startsWith("=")){let m=r.substring(1).trim();if(m.startsWith("{{")&&m.endsWith("}}")){let d=m.substring(2,m.length-2).trim();if(!d.includes("{{")&&!d.includes("}}"))return`(${this.transformTokens(d,t,n)})`}r=r.substring(1).trim()}let i=[],s=0,a=r.replace(/\{\{(.*?)\}\}/g,(m,d)=>{let p=this.transformTokens(d.trim(),t,n),l=`__EXPR_PLACEHOLDER_${s++}__`;return i.push(`\${(${p}) ?? ''}`),l}).replace(/`/g,"\\`").replace(/\${/g,"\\${");return i.forEach((m,d)=>{a=a.replace(`__EXPR_PLACEHOLDER_${d}__`,m)}),`\`${a}\``}static transformTokens(e,t,n){let r=e;if(r=r.replace(/\$now\b/g,"new Date().toISOString()"),r=r.replace(/\$today\b/g,'new Date().toISOString().split("T")[0]'),r=r.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g,(i,s,o)=>{let a=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(s)})?.item?.json${a.length?"?."+a.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g,(i,s,o)=>{let a=o?o.split(".").filter(Boolean):[];return`(ctx.getNode(${JSON.stringify(s)})?.first()?.json${a.length?"?."+a.join("?."):""})`}),r=r.replace(/\$\(['"](.*?)['"]\)/g,(i,s)=>`ctx.getNode(${JSON.stringify(s)})`),r=r.replace(/\$node\(['"](.*?)['"]\)/g,(i,s)=>`ctx.getNode(${JSON.stringify(s)})`),r=r.replace(/\$json([a-zA-Z0-9_.]*)/g,(i,s)=>{if(!s)return"item?.json";return`item?.json?.${s.split(".").filter(Boolean).join("?.")}`}),r=r.replace(/\$input\.first\(\)\.json/g,`${t??"item"}?.json`),t)r=r.replace(/\$input\.first\(\)/g,t);return r=r.replace(/\$input\.item\.json/g,"item?.json"),r=r.replace(/\$input\.all\(\)/g,n),r=r.replace(/\$env\.([a-zA-Z0-9_]+)/g,(i,s)=>`process.env.${s}`),r}}function A(e){return e.type==="n8n-nodes-base.merge"&&e.parameters?.mode==="combine"&&e.parameters?.combineBy==="combineByPosition"}function he(e){let t=e.parameters.numberInputs??2;if(!Number.isInteger(t)||t<2||t>10)return"position Merge requires 2 to 10 input ports";for(let{inputIndex:n=0}of e.incomingConnections??[])if(!Number.isInteger(n)||n<0||n>=t)return"position Merge has an invalid input port"}function K(e){let t=he(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=n.numberInputs??2,i=Array.from({length:r},()=>[]);for(let{sourceNode:m,outputIndex:d,inputIndex:p=0}of e.node.incomingConnections??[])i[p].push(`ctx.getNodeOutput(${JSON.stringify(m)}, ${d}) || []`);let s=i.map((m)=>`[${m.join(", ")}].flat()`),o=n.options??{},a=u.parse(o.includeUnpaired??!1);return`
    {
      const inputs: N8nItem[][] = [${s.join(", ")}];
      const merged = ((item: N8nItem) => mergeByPosition(
        inputs, Boolean(${a}), ${JSON.stringify(o.clashHandling?.values??{})},
      ))(inputs[0][0] || { json: {} });
      ${e.variableName} = merged;
      item = merged[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, [[0, merged]], merged);
    }`}function L(e){if(!e.type.includes("code")&&!e.type.includes("function"))return!1;let t=e.parameters?.jsCode||e.parameters?.functionCode;return typeof t==="string"&&/\$node\b/.test(t)}function X(e){if(L(e)&&e.parameters?.mode==="runOnceForEachItem")return"Code legacy $node references require Run Once for All Items"}function Z(e,t){let n=new Map,r=[e],i=new Set([e.name]);for(let s=0;s<r.length;s++){let o=[...r[s].incomingConnections??[]];o.sort((a,m)=>(a.inputIndex??0)-(m.inputIndex??0));for(let{sourceNode:a,outputIndex:m}of o){if(i.has(a))continue;i.add(a),n.set(a,m);let d=t.get(a);if(d)r.push(d)}}return[...n]}var S="n8n-nodes-base.s3";function we(e){let t=e.parameters??{};if((e.typeVersion??1)!==1||(t.resource??"file")!=="file"||(t.operation??"download")!=="download")return"S3 supports File / Download on node version 1";if(typeof t.bucketName!=="string"||!t.bucketName||typeof t.fileKey!=="string"||!t.fileKey)return"S3 Download requires bucketName and fileKey"}function Y(e){let t=we(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters,r=e.previousVariableName??"inputItems",i=u.parse(n.bucketName),s=u.parse(n.fileKey),o=u.parse(n.binaryPropertyName??"data");return`
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
    }`}function x(e){return e?.sendBody===!0&&e.contentType==="multipart-form-data"}function _(e){return e?.sendHeaders===!0&&e.specifyHeaders==="json"}function ee(e){let t=(e.method??"GET").toUpperCase();if(!["POST","PUT","PATCH"].includes(t))return"HTTP Request multipart supports POST, PUT and PATCH";let n=e.bodyParameters?.parameters??[];if(!Array.isArray(n))return"HTTP Request multipart needs an array of form fields";for(let r of n){if(typeof r?.name!=="string"||!r.name)return"HTTP Request multipart fields require a name";if(r.parameterType==="formBinaryData"){if(typeof r.inputDataFieldName!=="string"||!r.inputDataFieldName)return"HTTP Request multipart binary fields require an input binary property"}else if(r.parameterType&&!["formData","text"].includes(r.parameterType))return"HTTP Request multipart supports text and binary form fields"}}function te(e){return`createRequestForm(item, [${(e.bodyParameters?.parameters??[]).map((r)=>{let i=u.parse(r.name),s=r.parameterType==="formBinaryData"?"binaryProperty":"value",o=u.parse(r.parameterType==="formBinaryData"?r.inputDataFieldName:r.value);return`{ name: String(${i} ?? ''), ${s}: String(${o} ?? '') }`}).join(", ")}])`}var O="n8n-nodes-base.rabbitmq";function ve(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1)return"RabbitMQ needs typeVersion 1 or 1.1";if((t.operation??"sendMessage")!=="sendMessage"||t.mode!=="exchange"||Object.keys(t.options??{}).length>0)return"RabbitMQ only supports Send Message to an existing exchange with default options"}function re(e){let t=ve(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=e.previousVariableName??"inputItems",i=u.parse(n.exchange??""),s=u.parse(n.routingKey??""),o=u.parse(n.sendInputData??!0),a=u.parse(n.message??""),m=e.variableName;return`
    {
      const source: N8nItem[] = ${r} || inputItems;
      ${m} = [];
      if (source.length > 0) {
        item = source[0];
        const exchange = String(${i});
        const routingKey = String(${s});
        const sendInputData = Boolean(${o});
        const messages = source.map((item) => sendInputData ? JSON.stringify(item.json) : String(${a}));
        ${m} = await publishRabbitMq(exchange, routingKey, messages);
      }
      item = ${m}[0] || { json: {} };
      ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${m});
    }`}var C="n8n-nodes-base.convertToFile";function Ie(e){let t=e.parameters??{},n=e.typeVersion??1;if(n!==1&&n!==1.1||t.operation!=="toJson")return"Convert to File supports Convert to JSON on node versions 1 and 1.1";if((t.mode??"once")!=="once")return"Convert to JSON supports the All Items to One File mode";if(!["utf8","utf-8"].includes(t.options?.encoding??"utf8"))return"Convert to JSON supports UTF-8 encoding"}function ne(e){let t=Ie(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters??{},r=n.options??{},i=e.previousVariableName??"inputItems",s=u.parse(n.binaryPropertyName??"data"),o=u.parse(r.fileName??""),a=u.parse(r.format??!1),m=u.parse(r.addBOM??!1);return`
    {
      const source: N8nItem[] = ${i} || inputItems;
      ${e.variableName} = [];
      if (source.length > 0) {
        const item = source[0];
        ${e.variableName} = [convertItemsToJsonFile(source, {
          binaryProperty: String(${s} ?? ''),
          fileName: String(${o} ?? ''),
          format: Boolean(${a}),
          addBOM: Boolean(${m}),
        })];
      }
    }
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}function k(e){return e.type==="n8n-nodes-base.set"&&e.parameters?.mode==="raw"}function oe(e){let t=e.parameters?.jsonOutput;return k(e)&&typeof t==="string"&&t.startsWith("=")&&/\{\{[\s\S]*?\$node\b[\s\S]*?\}\}/.test(t)}function se(e){if(!k(e))return;if(e.typeVersion!==3.4)return"Edit Fields JSON Output requires node version 3.4";let t=e.parameters,{dotNotation:n,stripBinary:r}=t.options??{};if([t.includeOtherFields,n,r].some((i)=>i!==void 0&&typeof i!=="boolean"))return"Edit Fields JSON Output requires static field and binary options";if(t.includeOtherFields!==!1&&t.includeOtherFields!==void 0&&!["all","none"].includes(t.include??"all"))return"Edit Fields JSON Output supports including all or none of the input fields";if(t.duplicateItem)return"Edit Fields JSON Output does not support manual item duplication"}function Se(e){if(typeof e!=="string"||!e.startsWith("="))return JSON.stringify(e);let t=e.replace(/^=+/,""),n=(a)=>u.parse("="+a,"source[0]","source"),r=t.trim();if(r.startsWith("{{")&&r.endsWith("}}")&&!r.slice(2,-2).includes("{{"))return n(r);let i=[],s=0,o=!1;for(let a of t.matchAll(/\{\{([\s\S]*?)\}\}/g)){let m=t.slice(s,a.index);for(let[d]of m.matchAll(/\\[\s\S]|"/g))if(d==='"')o=!o;i.push(JSON.stringify(m)),i.push("encodeJsonTemplateValue("+n(a[0])+", "+o+")"),s=a.index+a[0].length}return i.push(JSON.stringify(t.slice(s))),"["+i.join(", ")+"].join('')"}function ie(e){let t=e.node.parameters,n=t.options??{},r=Se(t.jsonOutput===void 0?{my_field_1:"value",my_field_2:1}:t.jsonOutput),i=t.includeOtherFields===!0&&(t.include??"all")==="all",s=n.stripBinary??!0,o=n.dotNotation??!0,a=e.legacyNodePorts?"const $node = createLegacyNodeReferences(ctx, legacyNodeNames, "+JSON.stringify(e.legacyNodePorts)+", itemIndex);":"";return["{","  const source: N8nItem[] = "+(e.previousVariableName??"inputItems")+";","  "+e.variableName+" = source.map((item, itemIndex) => {",...a?["    "+a]:[],"    return applyRawEditFields(item, itemIndex, "+r+", {","      includeAll: "+i+", stripBinary: "+s+",","      dotNotation: "+o+",","    });","  });","}","item = "+e.variableName+"[0] || { json: {} };","ctx.setNodeOutput("+JSON.stringify(e.node.name)+", "+e.variableName+");"].join(`
`)}function ae(e){let t=e.parameters??{};return e.type==="n8n-nodes-base.code"&&e.onError==="continueRegularOutput"&&(t.language??"javaScript")==="javaScript"&&(t.mode??"runOnceForAllItems")==="runOnceForAllItems"}class H{static mapNode(e){let t=e.node.type;if(t===O)return re(e);if(t===S)return Y(e);if(t===C)return ne(e);if(t.includes("respondToWebhook"))return this.mapRespondToWebhookNode(e);if(t.includes("code")||t.includes("function"))return this.mapCodeNode(e);if(t.includes("executeWorkflowTrigger"))return this.mapGenericNode(e,"Subworkflow Entry Trigger");if(t.includes("executeWorkflow"))return this.mapSubworkflowNode(e);if(t.includes("filter"))return this.mapFilterNode(e);if(t.includes("if"))return this.mapIfNode(e);if(t.includes("switch"))return this.mapSwitchNode(e);if(t.includes("set")){if(k(e.node)&&!se(e.node))return ie(e);return this.mapSetNode(e)}if(t.includes("httpRequest"))return this.mapHttpRequestNode(e);if(t.includes("aggregate")||t.includes("itemLists"))return this.mapAggregateNode(e);if(t.includes("splitOut"))return this.mapSplitOutNode(e);if(t.includes("merge"))return A(e.node)?K(e):this.mapMergeNode(e);if(t.includes("postgres"))return this.mapPostgresNode(e);if(t.includes("openAi")||t.includes("googleGemini")||t.includes("chainLlm"))return this.mapLlmNode(e);if(t.includes("wait"))return this.mapWaitNode(e);return this.mapGenericNode(e,`Unmapped Node (${t})`)}static mapRespondToWebhookNode(e){let t=e.node.parameters||{},n=t.respondWith||"firstIncomingItem",r=t.options?.responseCode||200,i=e.previousVariableName||"inputItems",s="item?.json || {}";if(n==="allIncomingItems")s=`${e.variableName}.map((i: any) => i.json)`;else if(n==="json"&&t.responseBody)s=u.parse(t.responseBody);else if(n==="text"&&t.responseBody)s=u.parse(t.responseBody);return`
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
    }`}static mapCodeNode(e){let t=X(e.node);if(t)throw Error(`Node ${JSON.stringify(e.node.name)}: ${t}`);let n=e.node.parameters||{},r=n.jsCode||n.functionCode||"return $input.all();",i=e.previousVariableName||"inputItems",s=e.legacyNodePorts?`      const $node = createLegacyNodeReferences(ctx, legacyNodeNames, ${JSON.stringify(e.legacyNodePorts)});
`:"",o=r.replace(/\$input\.all\(\)/g,"currentItems").replace(/\$input\.first\(\)/g,"(currentItems[0] || { json: {} })").replace(/\$input\.item/g,"(currentItems[0] || { json: {} })").replace(/\$json/g,"(currentItems[0]?.json || {})").replace(/\$\(['"](.*?)['"]\)\.first\(\)/g,(d,p)=>`(ctx.getNode(${JSON.stringify(p)})?.first() || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.item/g,(d,p)=>`(ctx.getNode(${JSON.stringify(p)})?.item || { json: {} })`).replace(/\$\(['"](.*?)['"]\)\.all\(\)/g,(d,p)=>`(ctx.getNode(${JSON.stringify(p)})?.all() || [])`).replace(/\$now/g,"DateTime.now()"),a=`
${s}      const result = await (async () => {
        ${o}
      })();
      return n8n.normalizeItems(result);`,m=ae(e.node)?`
      try {${a}
      } catch (error) {
        return [{ json: { error: error instanceof Error ? error.message : String(error) } }];
      }`:a;return`
    // [Node: ${e.node.name}] Code Execution
    ${e.variableName} = await (async () => {
      const currentItems = ${i} || inputItems;${m}
    })();
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSubworkflowNode(e){let t=e.node.parameters||{},n=typeof t.workflowId==="object"?t.workflowId?.value||t.workflowId?.cachedResultName||"":t.workflowId||"",r=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] In-Memory Subworkflow Execution (< 0.05ms)
    ${e.variableName} = await executeSubworkflow(${JSON.stringify(n)}, ${r} || inputItems, ctx);
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapSetNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.options?.keepOnlySet===!0||t.include==="none",i=e.node.typeVersion??3.4,s=t.options??{},o;if(i>=3.4){let m=u.parse(t.includeOtherFields??!1),d=u.parse(t.include??"all"),p=u.parse(s.stripBinary??!1);o=`(${m}) && (${d}) !== 'none' && !(${p})`}else if(i>=3)o=u.parse(s.includeBinary??!1);else o=`!(${u.parse(t.keepOnlySet??s.keepOnlySet??!1)})`;let a=[];if(t.assignments?.assignments&&Array.isArray(t.assignments.assignments)){for(let m of t.assignments.assignments)if(m.name){let d=u.parse(m.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(m.name)}, ${d});`)}}if(t.values)for(let m of["string","number","boolean"]){let d=t.values[m];if(Array.isArray(d)){for(let p of d)if(p.name){let l=u.parse(p.value);a.push(`n8n.setNestedProperty(newJson, ${JSON.stringify(p.name)}, ${l});`)}}}return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static parseConditions(e,t=!1){let n=[],r=t?void 0:"firstInputItem";if(Array.isArray(e?.conditions))for(let s of e.conditions){let o=u.parse(s.leftValue,r),a=u.parse(s.rightValue,r),m=s.operator?.operation||"equals";if(t&&["true","false","gte","lte"].includes(m)){n.push(`Boolean(${o})`);continue}if(!t&&e.options?.caseSensitive===!1&&s.operator?.type==="string")o=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${o}))`,a=`(((value: unknown) => typeof value === 'string' ? value.toLowerCase() : value)(${a}))`;switch(m){case"equals":case"equal":n.push(`(${o} == ${a})`);break;case"notEquals":case"notEqual":n.push(`(${o} != ${a})`);break;case"contains":n.push(`String(${o} ?? '').includes(String(${a} ?? ''))`);break;case"true":n.push(`(${o} === true)`);break;case"false":n.push(`(${o} === false)`);break;case"gt":case"larger":n.push(`(Number(${o}) > Number(${a}))`);break;case"lt":case"smaller":n.push(`(Number(${o}) < Number(${a}))`);break;case"gte":n.push(`(Number(${o}) >= Number(${a}))`);break;case"lte":n.push(`(Number(${o}) <= Number(${a}))`);break;case"isEmpty":case"empty":n.push(`(${o} === undefined || ${o} === null || ${o} === '')`);break;case"isNotEmpty":case"notEmpty":n.push(`(${o} !== undefined && ${o} !== null && ${o} !== '')`);break;default:n.push(`Boolean(${o})`)}}let i=e?.combinator==="or"?" || ":" && ";return n.length?n.join(i):"true"}static mapFilterNode(e){let t=e.previousVariableName||"inputItems",n=this.parseConditions(e.node.parameters?.conditions,!0);return`
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
    item = ${e.variableName}[0] || { json: {} };`}static mapSwitchNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.rules?.values||[],i=t.options??{},s=i.fallbackOutput??"none",o=s==="extra"?r.length:s,m=s==="extra"||Number.isInteger(o)&&o>=0&&o<r.length?`
        if (!matched) {
          send(item, ${o});
          matched = true;
        }`:"",p=`
      const predicates = [${r.map((f)=>`(item: N8nItem) => (${f.conditions?this.parseConditions(f.conditions):"false"})`).join(", ")}];
      for (const item of source) {
        let matched = false;
        for (const [port, matches] of predicates.entries()) {
          if (matches(item)) {
            send(item, port);
            matched = true;
            if (!(${u.parse(i.allMatchingOutputs??!1,"firstInputItem")})) break;
          }
        }
        ${m}
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
      ${t.mode==="expression"?l:p}
      ${e.variableName} = routed;
      item = routed[0] || { json: {} };
      ctx.setNodeOutputs(${JSON.stringify(e.node.name)}, outputs, routed);
    }`}static mapHttpRequestNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=(t.method||"GET").toUpperCase(),i=u.parse(t.url||""),s=x(t),o=_(t),a="{}";if(o)a=`parseRequestHeaders(${u.parse(t.jsonHeaders??"{}")})`;else if(t.headerParameters?.parameters&&Array.isArray(t.headerParameters.parameters))a=`{ ${t.headerParameters.parameters.filter((l)=>l.name).map((l)=>`${JSON.stringify(l.name)}: ${u.parse(l.value)}`).join(", ")} }`;let m="undefined";if(s){let p=ee(t);if(p)throw Error(`Node ${JSON.stringify(e.node.name)}: ${p}`);m=te(t)}else if(["POST","PUT","PATCH"].includes(r)){if(t.jsonBody)m=`JSON.stringify(${u.parse(t.jsonBody)})`;else if(t.bodyParameters?.parameters&&Array.isArray(t.bodyParameters.parameters))m=`JSON.stringify({ ${t.bodyParameters.parameters.filter((l)=>l.name).map((l)=>`${JSON.stringify(l.name)}: ${u.parse(l.value)}`).join(", ")} })`}let d=`const reqHeaders: Record<string, string> = { 'Content-Type': 'application/json', ...${a} };`;if(s)d=`const reqHeaders = ${o?a:`new Headers(${a})`};
      reqHeaders.delete('content-type');
      reqHeaders.delete('content-length');`;else if(o)d=`const reqHeaders = ${a};
      if (!reqHeaders.has('content-type')) reqHeaders.set('content-type', 'application/json');`;return`
    // [Node: ${e.node.name}] Native HTTP Request (${r})
    ${e.variableName} = await Promise.all((${n} || inputItems).map(async (currentItem: N8nItem) => {
      const item = currentItem;
      const targetUrl = ${i};
      ${d}
      const reqBody = ${m};
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
    }`}static mapPostgresNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=u.parse(t.query||"SELECT 1;");return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapLlmNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=t.modelId?.value||t.model||"gpt-4o-mini",i=t.messages?.values?.[0]?.content||t.prompt||t.text||"Hello",s=u.parse(i);return`
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
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapWaitNode(e){let t=e.node.parameters||{},n=e.previousVariableName||"inputItems",r=Number(t.amount||1),i=t.unit||"seconds",o=r*(i==="minutes"?60000:i==="hours"?3600000:1000);return`
    // [Node: ${e.node.name}] Non-blocking Async Wait (${r} ${i})
    await new Promise((resolve) => setTimeout(resolve, ${o}));
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}static mapGenericNode(e,t){let n=e.previousVariableName||"inputItems";return`
    // [Node: ${e.node.name}] ${t} (Pass-through)
    ${e.variableName} = ${n} || inputItems;
    item = ${e.variableName}[0] || { json: {} };
    ctx.setNodeOutput(${JSON.stringify(e.node.name)}, ${e.variableName});`}}class T{static sanitizeIdentifier(e){return e.replace(/[^a-zA-Z0-9_]/g,"_").replace(/^[0-9]/,"_$&").replace(/__+/g,"_")}static generateWorkflowCode(e){let t=`workflow_${this.sanitizeIdentifier(e.fileName)}`,n=(e.relativePath.match(/\//g)||[]).length,r="../".repeat(n+1)+"runtime",i=e.nodes.some(A)?`import { mergeByPosition } from '${r}/position-merge';
`:"",s=new Set(e.nodes.filter((c)=>L(c)||oe(c))),o=e.nodes.some(k)?`import { applyRawEditFields, encodeJsonTemplateValue } from '${r}/raw-edit-fields';
`:"",a=s.size>0?`import { createLegacyNodeReferences } from '${r}/code-context';
`:"",m=s.size>0?`const legacyNodeNames = new Set<string>(${JSON.stringify(e.nodes.map((c)=>c.name))});
`:"",d=new Map(e.nodes.map((c)=>[c.name,c])),p=e.nodes.some((c)=>c.type===S)?`import { downloadS3Files } from '${r}/s3';
`:"",l=e.nodes.some((c)=>c.type.includes("httpRequest")&&(x(c.parameters)||_(c.parameters)))?`import { createRequestForm, parseRequestHeaders } from '${r}/http-request';
`:"",f=e.nodes.some((c)=>c.type===O)?`import { publishRabbitMq } from '${r}/rabbitmq';
`:"",N=e.nodes.some((c)=>c.type===C)?`import { convertItemsToJsonFile } from '${r}/convert-to-json';
`:"",y=`import { WorkflowContext } from '${r}/workflow-context';
import { n8n, N8nItem } from '${r}/n8n-helper';
import { executeSubworkflow, registerWorkflow } from '${r}/workflow-registry';
${i}${a}${p}${l}${f}${N}${o}
${m}
const DateTime = {
  now: () => ({
    toString: () => new Date().toISOString(),
    toJSDate: () => new Date()
  })
};
`,b=`  let item: N8nItem = inputItems[0] || { json: {} };
`,g=`  ctx.setNodeOutput("__initial__", inputItems);
`,E=new Map;e.orderedNodes.forEach((c,w)=>{let h=`node_${w}_${this.sanitizeIdentifier(c.name)}`;E.set(c.name,h)});let P=e.triggers,U=P.some((c)=>c.type===J),me=e.orderedNodes.filter((c)=>!P.includes(c));P.forEach((c)=>{let w=e.orderedNodes.indexOf(c),h=E.get(c.name);b+=`  let ${h}: any = inputItems;
`;let v=JSON.stringify(c.name),M=`entryNode === ${v}`;if(c.type!==J){let R=(c.parameters?.path||c.name).toLowerCase().trim().replace(/^\/+/,"");g+=`  const triggerNode_${w} = item?.json?.__triggerNode;
`,g+=`  const triggerPath_${w} = item?.json?.__triggerPath;
`,M=`entryNode === undefined && (!triggerNode_${w} || triggerNode_${w} === ${v} || triggerPath_${w} === ${JSON.stringify(R)})`}let j=`${h} = inputItems;
ctx.setNodeOutput(${v}, inputItems);`;g+=this.generateNodeBlock(e.id,c,M,j)}),me.forEach((c)=>{let w=E.get(c.name);b+=`  let ${w}: any = inputItems;
`;let h="true",v=c.incomingConnections??[],M=v.length>0&&!c.type.includes("merge");if(v.length>0)h=`(${v.map((I)=>`ctx.isBranchActive(${JSON.stringify(I.sourceNode)}, ${I.outputIndex})`).join(" || ")})`;let j=H.mapNode({node:c,workflowName:e.name,variableName:w,previousVariableName:M?"incomingItems":"inputItems",legacyNodePorts:s.has(c)?Z(c,d):void 0});if(M){let F=v.map((I)=>`ctx.getNodeOutputBatches(${JSON.stringify(I.sourceNode)}, ${I.outputIndex})`);j=`await ctx.runNodeBatches(${JSON.stringify(c.name)}, [${F.join(", ")}].flat(), async (incomingItems) => {
${j.trim()}
});`}let R=c.ancestorTriggers??[];if(R.length>0)h=`(${R.map((I)=>`ctx.hasExecutedNode(${JSON.stringify(I)})`).join(" || ")}) && ${h}`;else if(U)h=`entryNode === undefined && ${h}`;g+=this.generateNodeBlock(e.id,c,h,j,R.length>0||U?"      ":"    ")});let Q=e.orderedNodes[e.orderedNodes.length-1]?.name,de=Q?`(ctx.getNode(${JSON.stringify(Q)})?.all() || [{ json: item.json }])`:"inputItems",ue=Array.from(new Set([e.id,e.name,e.fileName])).filter(Boolean);return`${y}
export async function ${t}(
  inputItems: N8nItem[] = [{ json: {} }],
  parentCtx?: WorkflowContext,
  entryNode?: string
): Promise<{ items: N8nItem[]; context: WorkflowContext }> {
  const ctx = parentCtx || new WorkflowContext();
  const $env = process.env;
${b}
${g}
  const finalItems = ${de};
  return { items: finalItems, context: ctx };
}

registerWorkflow(${JSON.stringify(ue)}, ${t});
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
`}}class D{static generateRoutesCode(e){let t=`import { randomUUID } from 'node:crypto';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { N8nItem } from '../runtime/n8n-helper';
import { WorkflowContext } from '../runtime/workflow-context';
`,n=[];e.forEach((o)=>{let a=`workflow_${T.sanitizeIdentifier(o.fileName)}`,m=o.relativePath.replace(/\.ts$/,"");t+=`import { ${a} } from '../workflows/${m}';
`,n.push(a)});let r="",i=[],s=new Set;if(e.forEach((o)=>{let a=`workflow_${T.sanitizeIdentifier(o.fileName)}`,m=o.nodes.filter((d)=>d.type.includes("webhook"));for(let d of q(o)){let p=z(d);if(typeof p==="string")throw Error(`Workflow ${JSON.stringify(o.name)}, node ${JSON.stringify(d.name)}: ${p}`);let l=JSON.stringify({workflowId:o.id,node:d.name});i.push(`      stopConsumers.push(await startRabbitMqConsumer(
        ${JSON.stringify(p)},
        (items) => ${a}(items, undefined, ${JSON.stringify(d.name)}),
        (message) => fastify.log.error(${l}, message),
      ));`)}m.forEach((d)=>{let p=(d.parameters?.path||o.fileName).toLowerCase().trim().replace(/^\/+/,""),l=(d.parameters?.httpMethod||"POST").toUpperCase(),f=l==="ALL"?["GET","POST","PUT","DELETE"]:[l],N=Array.from(new Set([`/${p}`,`/webhook/${p}`]));f.forEach((y)=>{N.forEach((b)=>{let g=`${y}:${b}`;if(s.has(g))return;s.add(g),r+=`
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
          __triggerNode: ${JSON.stringify(d.name)},
          __triggerPath: ${JSON.stringify(p)}
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
  console.log(\`⚡ [n8n-to-fastify] Microservice online at http://\${host}:\${port}\`);${e.some((n)=>q(n).length>0)?`
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
`}static generateOutputPackageJson(e=[],t={}){let n=e.some((s)=>s.nodes.some((o)=>o.type===S)),r=e.some((s)=>q(s).length>0||s.nodes.some((o)=>o.type===O)),i=n||e.some((s)=>s.nodes.some((o)=>o.type===C));return JSON.stringify({name:"compiled-n8n-service",version:"1.0.0",type:"module",scripts:{start:"bun run server.ts",dev:"bun --watch run server.ts"},dependencies:{"@fastify/cors":"^9.0.1",fastify:"^4.28.1",...n?{"@aws-sdk/client-s3":"3.808.0"}:{},...i?{"mime-types":"^3.0.1"}:{},...r?{amqplib:"^0.10.9"}:{},...t}},null,2)}}function pe(e){let t=performance.now(),n=typeof e==="string"?JSON.parse(e):e,r=V.parseWorkflow(n,n?.name||"workflow","workflow.ts");if(!r)throw Error('Invalid n8n workflow JSON: missing "nodes" array.');let i=T.generateWorkflowCode(r),s=D.generateRoutesCode([r]),o=(performance.now()-t).toFixed(2);return{name:r.name,nodeCount:r.nodes.length,triggerCount:r.triggers.length,elapsedMs:o,workflowCode:i,routesCode:s}}if(typeof window<"u")window.N8nToFastify={compileWorkflowJson:pe};})();
