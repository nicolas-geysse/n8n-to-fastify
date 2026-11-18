import type { N8nConnectionTarget, N8nNode, ParsedWorkflow } from './core-parser';

interface Edge {
  source: string;
  target: string;
  output: number;
  input: number;
}

export interface PollingLoop {
  entry: string;
  nodes: string[];
  decision: string;
  repeatPort: number;
  backEdge: { source: string; output: number };
  exitPorts: number[];
  terminalExits: Array<{ output: number; target: string }>;
}

const waitType = 'n8n-nodes-base.wait';
const allowedTypes = new Set(['if', 'switch', 'wait', 'httpRequest', 'code', 'set']
  .map(type => 'n8n-nodes-base.' + type));

function readMainEdges(nodes: Map<string, N8nNode>, connections: ParsedWorkflow['connections']): Edge[] {
  const edges: Edge[] = [];
  for (const [source, outputs] of Object.entries(connections)) {
    if (!nodes.has(source) || !Array.isArray(outputs?.main)) continue;
    (outputs.main as N8nConnectionTarget[][]).forEach((targets, output) => {
      if (!Array.isArray(targets)) return;
      for (const target of targets) {
        if (target && nodes.has(target.node)) {
          edges.push({ source, output, target: target.node, input: target.index ?? 0 });
        }
      }
    });
  }
  return edges;
}

function cyclicComponents(nodes: Iterable<string>, outgoing: Map<string, Edge[]>): string[][] {
  const indices = new Map<string, number>();
  const low = new Map<string, number>();
  const stack: string[] = [];
  const onStack = new Set<string>();
  const result: string[][] = [];

  function visit(node: string) {
    const index = indices.size;
    indices.set(node, index);
    low.set(node, index);
    stack.push(node);
    onStack.add(node);

    for (const { target } of outgoing.get(node) ?? []) {
      if (!indices.has(target)) {
        visit(target);
        low.set(node, Math.min(low.get(node)!, low.get(target)!));
      } else if (onStack.has(target)) {
        low.set(node, Math.min(low.get(node)!, indices.get(target)!));
      }
    }
    if (low.get(node) !== index) return;

    const component: string[] = [];
    let member: string;
    do {
      member = stack.pop()!;
      onStack.delete(member);
      component.push(member);
    } while (member !== node);
    if (component.length > 1 || outgoing.get(node)?.some(edge => edge.target === node)) {
      result.push(component);
    }
  }

  for (const node of nodes) {
    if (!indices.has(node)) visit(node);
  }
  return result;
}

export function findPollingLoops(nodes: N8nNode[], connections: ParsedWorkflow['connections']): PollingLoop[] {
  const byName = new Map(nodes.map(node => [node.name, node]));
  const edges = readMainEdges(byName, connections);
  const outgoing = new Map<string, Edge[]>();
  const incoming = new Map<string, Edge[]>();
  for (const edge of edges) {
    if (!outgoing.has(edge.source)) outgoing.set(edge.source, []);
    if (!incoming.has(edge.target)) incoming.set(edge.target, []);
    outgoing.get(edge.source)!.push(edge);
    incoming.get(edge.target)!.push(edge);
  }

  return cyclicComponents(byName.keys(), outgoing).map(component => {
    const loop = planLoop(component, byName, outgoing, incoming);
    if (!loop) {
      throw new Error('[n8n-to-fastify] Cyclic dependency detected. Only single-entry polling loops ' +
        'with a timed Wait and one IF or Switch retry output are supported. Nodes: ' +
        component.slice().sort().map(node => JSON.stringify(node)).join(', '));
    }
    return loop;
  });
}

export function projectPollingSchedule(
  nodes: N8nNode[],
  connections: ParsedWorkflow['connections'],
  loops: PollingLoop[],
): { nodes: N8nNode[]; connections: ParsedWorkflow['connections'] } {
  const entries = new Map(loops.flatMap(loop => loop.nodes.map(node => [node, loop.entry] as const)));
  const projected: Record<string, { main: N8nConnectionTarget[][] }> = Object.create(null);
  for (const edge of readMainEdges(new Map(nodes.map(node => [node.name, node])), connections)) {
    const source = entries.get(edge.source) ?? edge.source;
    const target = entries.get(edge.target) ?? edge.target;
    if (source === target) continue;
    const { main } = projected[source] ??= { main: [[]] };
    main[0].push({ node: target, type: 'main', index: 0 });
  }
  return { nodes: nodes.filter(node => (entries.get(node.name) ?? node.name) === node.name), connections: projected };
}

function planLoop(
  component: string[],
  nodes: Map<string, N8nNode>,
  outgoing: Map<string, Edge[]>,
  incoming: Map<string, Edge[]>,
): PollingLoop | undefined {
  const members = new Set(component);
  const definitions = component.map(name => nodes.get(name)!);
  if (definitions.some(node => !allowedTypes.has(node.type) || node.disabled)) return;
  const decisions = definitions.filter(node => node.type === 'n8n-nodes-base.if' || node.type === 'n8n-nodes-base.switch');
  const waits = definitions.filter(node => node.type === waitType);
  if (decisions.length !== 1 || waits.length !== 1 ||
    (waits[0].parameters?.resume ?? 'timeInterval') !== 'timeInterval') return;
  if (decisions[0].parameters?.options?.allMatchingOutputs) return;
  const decision = decisions[0].name;

  const entries = component.flatMap(node => (incoming.get(node) ?? []).filter(edge => !members.has(edge.source)));
  if (entries.length !== 1 || entries[0].input !== 0) return;
  const entry = entries[0].target;
  const order = [entry];
  const exits: Edge[] = [];
  let backEdge: Edge | undefined;
  let repeatPort: number | undefined;
  for (let index = 0; index < component.length; index++) {
    const node = order[index];
    const all = outgoing.get(node) ?? [];
    const next = all.filter(edge => members.has(edge.target));
    if (next.length !== 1 || next[0].input !== 0 || (node !== decision && next[0].output !== 0)) return;
    const external = all.filter(edge => !members.has(edge.target));
    if (external.length > 0 && (node !== decision || external.some(edge => edge.output === next[0].output))) return;
    exits.push(...external);
    if (node === decision) repeatPort = next[0].output;
    if (next[0].target === entry) {
      if (index !== component.length - 1) return;
      backEdge = next[0];
    } else {
      order.push(next[0].target);
    }
  }
  if (!backEdge || repeatPort === undefined || exits.length === 0) return;

  const terminalExits: PollingLoop['terminalExits'] = [];
  const exitPorts = new Set<number>();
  for (const edge of exits) {
    const target = nodes.get(edge.target)!;
    if (target.type === 'n8n-nodes-base.stopAndError' && !target.disabled) {
      if (edge.input !== 0 || incoming.get(edge.target)?.length !== 1 || outgoing.get(edge.target)?.length) return;
      terminalExits.push({ output: edge.output, target: edge.target });
    }
    exitPorts.add(edge.output);
  }
  return {
    entry, nodes: order, decision, repeatPort, backEdge: { source: backEdge.source, output: backEdge.output },
    exitPorts: [...exitPorts].sort((a, b) => a - b), terminalExits,
  };
}
