import { N8nItem } from './n8n-helper';

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
  private outputPorts = new Map<string, Map<number, N8nItem[]>>();
  private outputBatches = new Map<string, Map<number, N8nItem[][]>>();
  private activeBranches = new Map<string, Set<number>>();
  private executedNodes = new Set<string>();
  public webhookResponse?: { statusCode: number; body: any; headers?: Record<string, string> };

  public setNodeOutput(nodeName: string, items: N8nItem[], activeOutputIndex: number = 0): void {
    this.outputPorts.delete(nodeName);
    this.outputBatches.delete(nodeName);
    this.nodeOutputs.set(nodeName, items);
    this.executedNodes.add(nodeName);
    this.activateBranch(nodeName, activeOutputIndex);
  }

  public setNodeOutputs(nodeName: string, outputs: Iterable<[number, N8nItem[]]>, items: N8nItem[]): void {
    this.activeBranches.delete(nodeName);
    this.outputBatches.delete(nodeName);
    const ports = new Map<number, N8nItem[]>();
    for (const [port, output] of outputs) {
      if (output.length > 0) {
        this.setNodeOutput(nodeName, output, port);
        ports.set(port, output);
      }
    }
    this.outputPorts.set(nodeName, ports);
    this.nodeOutputs.set(nodeName, items);
    this.executedNodes.add(nodeName);
  }

  public getNodeOutput(nodeName: string, outputIndex: number): N8nItem[] | undefined {
    if (!this.isBranchActive(nodeName, outputIndex)) return;
    const ports = this.outputPorts.get(nodeName);
    return ports ? ports.get(outputIndex) : this.nodeOutputs.get(nodeName);
  }

  public getNodeOutputBatches(nodeName: string, outputIndex: number): N8nItem[][] {
    const batches = this.outputBatches.get(nodeName)?.get(outputIndex);
    if (batches) return batches;
    const items = this.getNodeOutput(nodeName, outputIndex);
    return items === undefined ? [] : [items];
  }

  public async runNodeBatches(
    nodeName: string,
    batches: N8nItem[][],
    run: (items: N8nItem[]) => Promise<void>,
  ): Promise<void> {
    if (batches.length < 2) {
      if (batches.length === 1) await run(batches[0]);
      return;
    }

    const byPort = new Map<number, N8nItem[][]>();
    const all: N8nItem[][] = [];
    for (const input of batches) {
      this.nodeOutputs.delete(nodeName);
      this.outputPorts.delete(nodeName);
      this.outputBatches.delete(nodeName);
      this.activeBranches.delete(nodeName);
      await run(input);

      const items = this.nodeOutputs.get(nodeName);
      if (items?.length) all.push(items);
      for (const port of this.activeBranches.get(nodeName) ?? []) {
        const output = this.getNodeOutput(nodeName, port);
        if (!output?.length) continue;
        let portBatches = byPort.get(port);
        if (!portBatches) {
          portBatches = [];
          byPort.set(port, portBatches);
        }
        portBatches.push(output);
      }
    }
    this.setNodeOutputs(nodeName, [...byPort].map(([port, runs]) => [port, runs.flat()]), all.flat());
    this.outputBatches.set(nodeName, byPort);
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
