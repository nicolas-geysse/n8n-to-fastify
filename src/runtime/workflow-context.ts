import { N8nItem } from './n8n-helper';
import { WorkflowTrace } from './workflow-trace';
import type { NodeTraceSink } from './workflow-trace';

export interface NodeAccessor {
  item?: N8nItem;
  first: () => N8nItem | undefined;
  all: () => N8nItem[];
}

/**
 * Runtime execution context for a single compiled workflow invocation.
 * Tracks node outputs, active conditional branches (IF/Switch), and webhook reply state.
 */
export class WorkflowContext {
  private nodeOutputs = new Map<string, N8nItem[]>();
  private activeBranches = new Map<string, Set<number>>();
  private executedNodes = new Set<string>();
  private readonly trace?: WorkflowTrace;
  public webhookResponse?: { statusCode: number; body: any; headers?: Record<string, string> };

  constructor(traceSink?: NodeTraceSink) {
    if (!traceSink && process.env.N8N_TO_FASTIFY_TRACE === '1') {
      traceSink = event => console.info(JSON.stringify(event));
    }
    if (traceSink) this.trace = new WorkflowTrace(traceSink);
  }

  public startNodeTrace(workflowId: string, nodeId: string, nodeName: string) {
    return this.trace?.startNode({ workflowId, nodeId, nodeName });
  }

  public skipNodeTrace(workflowId: string, nodeId: string, nodeName: string): void {
    this.trace?.skipNode({ workflowId, nodeId, nodeName });
  }

  public setNodeOutput(nodeName: string, items: N8nItem[], activeOutputIndex: number = 0): void {
    this.nodeOutputs.set(nodeName, items);
    this.executedNodes.add(nodeName);
    this.activateBranch(nodeName, activeOutputIndex);
    this.trace?.recordOutput(nodeName, activeOutputIndex, items.length);
  }

  public activateBranch(nodeName: string, outputIndex: number): void {
    if (!this.activeBranches.has(nodeName)) {
      this.activeBranches.set(nodeName, new Set());
    }
    this.activeBranches.get(nodeName)!.add(outputIndex);
  }

  public isBranchActive(nodeName: string, outputIndex: number): boolean {
    return this.activeBranches.get(nodeName)?.has(outputIndex) ?? false;
  }

  public hasExecutedNode(nodeName: string): boolean {
    return this.executedNodes.has(nodeName);
  }

  public getNode(nodeName: string): NodeAccessor {
    const items = this.nodeOutputs.get(nodeName) || [];
    return {
      item: items[0],
      first: () => items[0],
      all: () => items,
    };
  }

  public setWebhookResponse(statusCode: number, body: any, headers?: Record<string, string>): void {
    if (!this.webhookResponse) {
      this.webhookResponse = { statusCode, body, headers };
    }
  }
}
