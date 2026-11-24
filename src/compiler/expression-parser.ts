/**
 * Expression Parser
 * Transpiles n8n expressions (e.g., `={{ $json.user.id }}` or `={{ $('NodeName').item.json.token }}`)
 * into safe native JavaScript/TypeScript with deep optional chaining (?.).
 */
export class ExpressionParser {
  static parse(expression: any, firstInputItem?: string, inputItems = 'inputItems'): string {
    if (expression === undefined || expression === null) {
      return JSON.stringify('');
    }
    if (typeof expression !== 'string') {
      return JSON.stringify(expression);
    }

    let expr = expression.trim();

    if (!expr.includes('{{')) {
      return JSON.stringify(expression);
    }

    // Pure single expression (e.g., ={{ $json.amount * 100 }})
    if (expr.startsWith('=')) {
      const innerContent = expr.substring(1).trim();
      if (innerContent.startsWith('{{') && innerContent.endsWith('}}')) {
        const between = innerContent.substring(2, innerContent.length - 2).trim();
        if (!between.includes('{{') && !between.includes('}}')) {
          const transformed = this.transformTokens(between, firstInputItem, inputItems);
          return `(${transformed})`;
        }
      }
      expr = expr.substring(1).trim();
    }

    const replacements: string[] = [];
    let placeholderIdx = 0;

    const tokenized = expr.replace(/\{\{(.*?)\}\}/g, (_, inner) => {
      const transformed = this.transformTokens(inner.trim(), firstInputItem, inputItems);
      const placeholder = `__EXPR_PLACEHOLDER_${placeholderIdx++}__`;
      replacements.push(`\${(${transformed}) ?? ''}`);
      return placeholder;
    });

    let safeLiteral = tokenized.replace(/`/g, '\\`').replace(/\${/g, '\\${');

    replacements.forEach((rep, idx) => {
      safeLiteral = safeLiteral.replace(`__EXPR_PLACEHOLDER_${idx}__`, rep);
    });

    return `\`${safeLiteral}\``;
  }

  private static transformTokens(code: string, firstInputItem: string | undefined, inputItems: string): string {
    let result = code;

    result = result.replace(/\$now\b(?:\s*\.\s*toISO\s*\(\s*\))?/g, 'new Date().toISOString()');
    result = result.replace(/\$today\b/g, 'new Date().toISOString().split("T")[0]');

    // $('NodeName').item.json.path -> ctx.getNode("NodeName")?.item?.json?.path
    result = result.replace(/\$\(['"](.*?)['"]\)\.item\.json([a-zA-Z0-9_.]*)/g, (_, nodeName, path) => {
      const parts = path ? path.split('.').filter(Boolean) : [];
      return `(ctx.getNode(${JSON.stringify(nodeName)})?.item?.json${parts.length ? '?.' + parts.join('?.') : ''})`;
    });

    // $('NodeName').first().json.path -> ctx.getNode("NodeName")?.first()?.json?.path
    result = result.replace(/\$\(['"](.*?)['"]\)\.first\(\)\.json([a-zA-Z0-9_.]*)/g, (_, nodeName, path) => {
      const parts = path ? path.split('.').filter(Boolean) : [];
      return `(ctx.getNode(${JSON.stringify(nodeName)})?.first()?.json${parts.length ? '?.' + parts.join('?.') : ''})`;
    });

    result = result.replace(/\$\(['"](.*?)['"]\)/g, (_, nodeName) => {
      return `ctx.getNode(${JSON.stringify(nodeName)})`;
    });
    result = result.replace(/\$node\(['"](.*?)['"]\)/g, (_, nodeName) => {
      return `ctx.getNode(${JSON.stringify(nodeName)})`;
    });

    // $json.a.b -> item?.json?.a?.b
    result = result.replace(/\$json([a-zA-Z0-9_.]*)/g, (_, path) => {
      if (!path) return 'item?.json';
      const parts = path.split('.').filter(Boolean);
      return `item?.json?.${parts.join('?.')}`;
    });

    result = result.replace(/\$input\.first\(\)\.json/g, `${firstInputItem ?? 'item'}?.json`);
    if (firstInputItem) result = result.replace(/\$input\.first\(\)/g, firstInputItem);
    result = result.replace(/\$input\.item\.json/g, 'item?.json');
    result = result.replace(/\$input\.all\(\)/g, inputItems);

    // Environment variables: $env.MY_KEY -> process.env.MY_KEY
    result = result.replace(/\$env\.([a-zA-Z0-9_]+)/g, (_, envVar) => {
      return `process.env.${envVar}`;
    });

    result = this.rewriteBinaryReferences(result);

    return result;
  }

  /**
   * Rewrites $binary expressions (e.g. $binary.data.fileName or $binary["key"]?.mimeType)
   * to safe optional chaining (e.g. item?.binary?.data?.fileName or item?.binary?.["key"]?.mimeType)
   * using a single-pass lexical scanner without external dependencies.
   */
  private static rewriteBinaryReferences(code: string): string {
    if (!code.includes('$binary')) return code;

    let out = '';
    let i = 0;
    const len = code.length;
    // Tracks brace nesting inside template literal `${...}` expressions.
    const templateBraceStack: number[] = [];

    const isIdentChar = (ch: string) => /[a-zA-Z0-9_$]/.test(ch);
    const skipWs = (pos: number) => {
      while (pos < len && /\s/.test(code[pos])) pos++;
      return pos;
    };
    const prevNonWsChar = (str: string) => {
      for (let p = str.length - 1; p >= 0; p--) {
        if (!/\s/.test(str[p])) return str[p];
      }
      return '';
    };

    while (i < len) {
      const ch = code[i];

      // Line comment
      if (ch === '/' && code[i + 1] === '/') {
        const end = code.indexOf('\n', i);
        const sliceEnd = end === -1 ? len : end + 1;
        out += code.slice(i, sliceEnd);
        i = sliceEnd;
        continue;
      }

      // Block comment
      if (ch === '/' && code[i + 1] === '*') {
        const end = code.indexOf('*/', i + 2);
        const sliceEnd = end === -1 ? len : end + 2;
        out += code.slice(i, sliceEnd);
        i = sliceEnd;
        continue;
      }

      // Single or double quoted string
      if (ch === '\'' || ch === '"') {
        const quote = ch;
        let j = i + 1;
        while (j < len) {
          if (code[j] === '\\') { j += 2; continue; }
          if (code[j] === quote) { j++; break; }
          j++;
        }
        out += code.slice(i, j);
        i = j;
        continue;
      }

      // Template literal (scan static segments verbatim, recurse into ${...})
      if (ch === '`') {
        out += '`';
        i++;
        while (i < len) {
          if (code[i] === '\\') {
            out += code.slice(i, i + 2);
            i += 2;
            continue;
          }
          if (code[i] === '$' && code[i + 1] === '{') {
            out += '${';
            i += 2;
            templateBraceStack.push(1);
            break;
          }
          if (code[i] === '`') {
            out += '`';
            i++;
            break;
          }
          out += code[i];
          i++;
        }
        continue;
      }

      // Handle closing `}` of a template `${...}` interpolation
      if (templateBraceStack.length > 0) {
        if (ch === '{') {
          templateBraceStack[templateBraceStack.length - 1]++;
        } else if (ch === '}') {
          templateBraceStack[templateBraceStack.length - 1]--;
          if (templateBraceStack[templateBraceStack.length - 1] === 0) {
            templateBraceStack.pop();
            out += '}';
            i++;
            // Resume scanning remainder of template literal
            while (i < len) {
              if (code[i] === '\\') {
                out += code.slice(i, i + 2);
                i += 2;
                continue;
              }
              if (code[i] === '$' && code[i + 1] === '{') {
                out += '${';
                i += 2;
                templateBraceStack.push(1);
                break;
              }
              if (code[i] === '`') {
                out += '`';
                i++;
                break;
              }
              out += code[i];
              i++;
            }
            continue;
          }
        }
      }

      // Regex literal heuristic: `/` preceded by operator/delimiter/start (not identifier/number/`)`/]`)
      if (ch === '/') {
        const prev = prevNonWsChar(out);
        const canStartRegex = !prev || '({[,;=!&|?:~+-*%^<>'.includes(prev);
        if (canStartRegex) {
          let j = i + 1;
          let inClass = false;
          while (j < len) {
            if (code[j] === '\\') { j += 2; continue; }
            if (code[j] === '[') inClass = true;
            else if (code[j] === ']') inClass = false;
            else if (code[j] === '/' && !inClass) {
              j++;
              while (j < len && /[gimsuvy]/.test(code[j])) j++;
              break;
            }
            j++;
          }
          out += code.slice(i, j);
          i = j;
          continue;
        }
      }

      // Standalone `$binary` identifier
      if (
        code.startsWith('$binary', i) &&
        (i === 0 || !isIdentChar(code[i - 1])) &&
        (i + 7 >= len || !isIdentChar(code[i + 7]))
      ) {
        const prev = prevNonWsChar(out);
        const afterPos = skipWs(i + 7);
        const next = code[afterPos] ?? '';

        // Property access on another object (e.g. `obj.$binary`)
        if (prev === '.') {
          out += '$binary';
          i += 7;
          continue;
        }

        const isObjectEntry = prev === '{' || prev === ',';
        // Object literal key `{ $binary: ... }`
        if (isObjectEntry && next === ':') {
          out += '$binary';
          i += 7;
          continue;
        }
        // Object literal shorthand `{ $binary }`
        if (isObjectEntry && (next === ',' || next === '}')) {
          out += '$binary: item?.binary';
          i += 7;
          continue;
        }

        out += 'item?.binary';
        i += 7;

        // Rewrite chained `.prop` / `[key]` / `?.prop` / `?.[key]` accessors
        while (i < len) {
          if (code[i] === '?' && code[i + 1] === '.') {
            i += 2;
            if (code[i] === '[') {
              out += '?.[';
              i++;
              break;
            }
            const start = i;
            while (i < len && /[a-zA-Z0-9_$]/.test(code[i])) i++;
            if (i > start) {
              out += '?.' + code.slice(start, i);
              continue;
            }
            out += '?.';
            break;
          }
          if (code[i] === '.') {
            const start = i + 1;
            let end = start;
            while (end < len && /[a-zA-Z0-9_$]/.test(code[end])) end++;
            if (end > start) {
              out += '?.' + code.slice(start, end);
              i = end;
              continue;
            }
            break;
          }
          if (code[i] === '[') {
            out += '?.[';
            i++;
            break;
          }
          break;
        }
        continue;
      }

      out += ch;
      i++;
    }

    return out;
  }
}
