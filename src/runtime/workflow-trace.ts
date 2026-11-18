import { randomUUID } from 'node:crypto';

export interface TraceNode {
  workflowId: string;
  nodeId: string;
  nodeName: string;
}

interface TraceOutput {
  port: number;
  itemCount: number;
}

export type NodeTraceEvent = TraceNode & {
  type: 'workflow.node';
  schemaVersion: 1;
  executionId: string;
} & (
  | { status: 'started' | 'skipped' }
  | { status: 'error'; durationMs: number }
  | { status: 'completed'; durationMs: number; outputs: TraceOutput[] }
);

export type NodeTraceSink = (event: NodeTraceEvent) => void | Promise<void>;

export class WorkflowTrace {
  private readonly executionId = randomUUID();
  private activeNode?: { name: string; outputs: Map<number, number> };

  constructor(private readonly sink: NodeTraceSink) {}

  public startNode(node: TraceNode): (status: 'completed' | 'error') => void {
    const previous = this.activeNode;
    const current = { name: node.nodeName, outputs: new Map<number, number>() };
    this.activeNode = current;
    const metadata = this.metadata(node);
    const start = performance.now();
    this.emit({ ...metadata, status: 'started' });

    return status => {
      this.activeNode = previous;
      const durationMs = performance.now() - start;
      if (status === 'completed') {
        const outputs = Array.from(current.outputs, ([port, itemCount]) => ({ port, itemCount }));
        this.emit({ ...metadata, status, durationMs, outputs });
      } else {
        this.emit({ ...metadata, status, durationMs });
      }
    };
  }

  public skipNode(node: TraceNode): void {
    this.emit({ ...this.metadata(node), status: 'skipped' });
  }

  public recordOutput(nodeName: string, port: number, itemCount: number): void {
    if (this.activeNode?.name === nodeName) {
      this.activeNode.outputs.set(port, itemCount);
    }
  }

  private metadata(node: TraceNode) {
    return {
      type: 'workflow.node' as const,
      schemaVersion: 1 as const,
      executionId: this.executionId,
      workflowId: node.workflowId,
      nodeId: node.nodeId,
      nodeName: node.nodeName,
    };
  }

  private emit(event: NodeTraceEvent): void {
    try {
      // A logging sink can fail independently of the workflow.
      void Promise.resolve(this.sink(event)).catch(() => {});
    } catch {}
  }
}
