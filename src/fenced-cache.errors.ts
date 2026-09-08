/** Every error message this library can produce. Nothing else builds message strings. */
export const FENCED_CACHE_ERRORS = {
  invalidNamespace: (namespace: unknown): string =>
    `namespace must be a non-empty string without "*", "{" or "}", received: ${String(namespace)}`,
  invalidGeneration: (raw: unknown): string =>
    `generation must be a cache-fence UUID token, received: ${String(raw)}`,
  invalidReadReply: (): string => 'Redis returned an invalid cache snapshot',
  invalidWriteReply: (): string => 'Redis returned an invalid fenced write result',
  invalidSweepReply: (): string => 'Redis returned an invalid cleanup result',
  invalidTtl: (option: string, value: unknown): string =>
    `${option} must be a positive integer number of milliseconds, received: ${String(value)}`,
  unserializableValue: (): string =>
    'value has no JSON representation and cannot be cached: undefined, functions and symbols serialize to nothing',
} as const;
