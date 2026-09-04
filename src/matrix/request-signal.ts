/** Combine the SDK's cancellation with our bounded network deadline. */
export function boundedRequestSignal(upstream: AbortSignal | undefined, timeoutMs: number) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  return {
    // Native composition avoids accumulating listeners on the SDK signal and
    // preserves cancellation during body reads, after fetch returns headers.
    signal: upstream ? AbortSignal.any([upstream, controller.signal]) : controller.signal,
    dispose() {
      clearTimeout(timeout);
    },
  };
}
