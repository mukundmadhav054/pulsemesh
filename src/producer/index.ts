/**
 * Ingestion API: validation (zod-style), sliding-window rate limiting,
 * mandatory idempotency keys.
 */
import { InMemoryStreams } from '../broker/streams';
import { encodeEnvelope } from '../schemas';

export class IdempotencyKeyRequiredError extends Error {
  constructor() {
    super('idempotencyKey is required');
    this.name = 'IdempotencyKeyRequiredError';
  }
}

export class ValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

export class RateLimitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RateLimitError';
  }
}

export interface IngestInput {
  type: string;
  partitionKey: string;
  payload: unknown;
  idempotencyKey?: string;
  /** Rate-limit bucket (e.g. API key or tenant id). Defaults to "default". */
  bucket?: string;
}

export interface IngestResult {
  entryId: string;
  deduplicated: boolean;
  stream: string;
}

/** Minimal zod-style string schema used for request validation. */
function stringSchema(
  value: unknown,
  field: string,
  opts: { min?: number; max?: number; required?: boolean } = {},
): string {
  const { min = 1, max = 256, required = true } = opts;
  if (value === undefined || value === null || value === '') {
    if (!required) return '';
    throw new ValidationError(`${field} is required`);
  }
  if (typeof value !== 'string') throw new ValidationError(`${field} must be a string`);
  if (value.length < min) throw new ValidationError(`${field} must not be empty`);
  if (value.length > max) throw new ValidationError(`${field} must be <= ${max} chars`);
  return value;
}

/** Sliding-window rate limiter (fixed window of timestamps per bucket). */
export class SlidingWindowRateLimiter {
  private hits = new Map<string, number[]>();

  constructor(
    private readonly maxRequests: number = 100,
    private readonly windowMs: number = 60_000,
  ) {}

  check(bucket: string, now = Date.now()): void {
    const cutoff = now - this.windowMs;
    const kept = (this.hits.get(bucket) ?? []).filter((t) => t > cutoff);
    if (kept.length >= this.maxRequests) {
      throw new RateLimitError(
        `rate limit exceeded for bucket "${bucket}" (${this.maxRequests}/${this.windowMs}ms)`,
      );
    }
    kept.push(now);
    this.hits.set(bucket, kept);
  }

  reset(): void {
    this.hits.clear();
  }
}

export interface ProducerOptions {
  stream?: string;
  rateLimitMax?: number;
  rateLimitWindowMs?: number;
}

export class IngestService {
  private readonly limiter: SlidingWindowRateLimiter;
  private readonly stream: string;

  constructor(
    private readonly streams: InMemoryStreams,
    opts: ProducerOptions = {},
  ) {
    this.stream = opts.stream ?? 'pulsemesh:tasks';
    this.limiter = new SlidingWindowRateLimiter(
      opts.rateLimitMax ?? 1000,
      opts.rateLimitWindowMs ?? 60_000,
    );
  }

  ingest(input: IngestInput): IngestResult {
    if (!input.idempotencyKey) throw new IdempotencyKeyRequiredError();
    const type = stringSchema(input.type, 'type', { max: 64 });
    const partitionKey = stringSchema(input.partitionKey, 'partitionKey', { max: 128 });
    const idempotencyKey = stringSchema(input.idempotencyKey, 'idempotencyKey', {
      max: 128,
    });
    if (typeof input.payload !== 'object' || input.payload === null) {
      throw new ValidationError('payload must be an object');
    }

    this.limiter.check(input.bucket ?? 'default');

    const body = encodeEnvelope({ type, idempotencyKey, partitionKey, payload: input.payload });
    const entryId = this.streams.xadd(this.stream, {
      idempotencyKey,
      partitionKey,
      body,
      type,
    });
    return { entryId, deduplicated: false, stream: this.stream };
  }

  get rateLimiter(): SlidingWindowRateLimiter {
    return this.limiter;
  }
}
