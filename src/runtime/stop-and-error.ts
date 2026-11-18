export class WorkflowStopError extends Error {
  constructor(message: string, readonly metadata?: Record<string, unknown>) {
    super(message);
    this.name = 'WorkflowStopError';
  }
}

export function createWorkflowStopError(
  type: 'errorMessage' | 'errorObject',
  value: unknown,
): WorkflowStopError {
  if (type === 'errorMessage') return new WorkflowStopError(String(value ?? ''));

  let data: unknown = value;
  if (typeof value === 'string') {
    try {
      data = JSON.parse(value);
    } catch {
      return new WorkflowStopError('Stop and Error expects a valid JSON object');
    }
  }
  if (data === null || typeof data !== 'object' || Array.isArray(data)) {
    return new WorkflowStopError('Stop and Error expects a valid JSON object');
  }
  const metadata = data as Record<string, unknown>;
  const message = [metadata.message, metadata.description, metadata.error]
    .find(value => typeof value === 'string' && value.length > 0);
  return new WorkflowStopError(typeof message === 'string' ? message : `Error: ${JSON.stringify(data)}`, metadata);
}
