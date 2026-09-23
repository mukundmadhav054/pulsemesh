/** Typed payload envelope — decouples serialization from transport. */

export const PAYLOAD_VERSION = 1;

export interface TaskEnvelope<T = unknown> {
  /** Schema version for forward-compatible decoding. */
  v: number;
  /** Task type discriminator. */
  type: string;
  /** Idempotency key (required end-to-end). */
  idempotencyKey: string;
  /** Partition key used for stream sharding. */
  partitionKey: string;
  /** Epoch ms when the envelope was created. */
  createdAt: number;
  /** The task payload. */
  payload: T;
}

export function encodeEnvelope<T>(input: {
  type: string;
  idempotencyKey: string;
  partitionKey: string;
  payload: T;
}): string {
  const envelope: TaskEnvelope<T> = {
    v: PAYLOAD_VERSION,
    type: input.type,
    idempotencyKey: input.idempotencyKey,
    partitionKey: input.partitionKey,
    createdAt: Date.now(),
    payload: input.payload,
  };
  return JSON.stringify(envelope);
}

export class EnvelopeDecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EnvelopeDecodeError';
  }
}

export function decodeEnvelope<T = unknown>(raw: string): TaskEnvelope<T> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new EnvelopeDecodeError('envelope is not valid JSON');
  }
  if (typeof parsed !== 'object' || parsed === null) {
    throw new EnvelopeDecodeError('envelope must be a JSON object');
  }
  const env = parsed as Record<string, unknown>;
  if (env['v'] !== PAYLOAD_VERSION) {
    throw new EnvelopeDecodeError(`unsupported envelope version: ${String(env['v'])}`);
  }
  if (typeof env['type'] !== 'string' || env['type'].length === 0) {
    throw new EnvelopeDecodeError('envelope.type must be a non-empty string');
  }
  if (typeof env['idempotencyKey'] !== 'string' || env['idempotencyKey'].length === 0) {
    throw new EnvelopeDecodeError('envelope.idempotencyKey is required');
  }
  if (typeof env['partitionKey'] !== 'string' || env['partitionKey'].length === 0) {
    throw new EnvelopeDecodeError('envelope.partitionKey is required');
  }
  return parsed as TaskEnvelope<T>;
}
