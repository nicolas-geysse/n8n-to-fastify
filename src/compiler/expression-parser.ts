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
   * using native string parsing without external dependencies like js-tokens.
   */
  private static rewriteBinaryReferences(code: string): string {
    if (!code.includes('$binary')) return code;

    // Tokenize strings, template literals, and regexes to protect them from replacement
    const protectedLiterals: string[] = [];
    let cleanCode = code.replace(/(["'])(?:\\(?:\r\n|[\s\S])|(?!\1)[^\\\r\n])*\1|`(?:\\[\s\S]|[^`\\])*`|\/(?![*\/])(?:\\.|\[(?:\\.|[^\]\\])*\]|[^\/\\\r\n])+\/[gimuy]*/g, (match) => {
      const token = `__LITERAL_PROTECTED_${protectedLiterals.length}__`;
      protectedLiterals.push(match);
      return token;
    });

    // 1. $binary.prop.sub -> item?.binary?.prop?.sub
    cleanCode = cleanCode.replace(/(?<![.\w$])\$binary((?:\.[a-zA-Z0-9_]+|\[[^\]]+\])*)/g, (_, accessors) => {
      if (!accessors) return 'item?.binary';
      // Convert .prop to ?.prop and [key] to ?.[key]
      const safeAccessors = accessors
        .replace(/\.([a-zA-Z0-9_]+)/g, '?.$1')
        .replace(/(\??\.?\[)/g, '?.[');
      return `item?.binary${safeAccessors.startsWith('?.') ? '' : '?.'}${safeAccessors}`;
    });

    // Restore protected literals
    protectedLiterals.forEach((lit, idx) => {
      cleanCode = cleanCode.replace(`__LITERAL_PROTECTED_${idx}__`, lit);
    });

    return cleanCode;
  }
}
