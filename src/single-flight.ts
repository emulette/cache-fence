/**
 * In-process deduplication of concurrent async work by key.
 *
 * Callers arriving while a task for the same key is running share its promise
 * instead of starting a second one. This removes duplicate work inside one
 * process; it says nothing about other processes, and it is not a lock.
 */
export class SingleFlight {
  private readonly inFlight = new Map<string, Promise<unknown>>();

  /**
   * Runs `task` for `key`, or returns the promise of the flight already running for it.
   *
   * The caller is responsible for the returned promise's rejection, exactly as if
   * it had called `task` itself.
   */
  run<T>(key: string, task: () => Promise<T>): Promise<T> {
    const existing = this.inFlight.get(key);
    if (existing !== undefined) {
      return existing as Promise<T>;
    }
    const promise = task().finally(() => {
      // Only drop our own entry: a later flight may already have taken this key.
      if (this.inFlight.get(key) === promise) {
        this.inFlight.delete(key);
      }
    });
    this.inFlight.set(key, promise);
    return promise;
  }

  /** Number of flights currently running. Intended for tests and diagnostics. */
  get size(): number {
    return this.inFlight.size;
  }
}
