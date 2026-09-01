export type SyncStateValue = string | null | undefined;

export type SubscribeToSync = (
  listener: (state: SyncStateValue) => void
) => () => void;

export function isUsableSyncState(state: SyncStateValue): boolean {
  return state === "PREPARED" || state === "SYNCING";
}

export async function waitForUsableSync(
  getCurrentState: () => SyncStateValue,
  subscribe: SubscribeToSync,
  timeoutMs: number
): Promise<void> {
  if (isUsableSyncState(getCurrentState())) return;

  await new Promise<void>((resolve, reject) => {
    let settled = false;
    let unsubscribe = () => {};
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      unsubscribe();
      if (error) reject(error);
      else resolve();
    };
    const timeout = setTimeout(
      () => finish(new Error(`Matrix initial sync timed out after ${timeoutMs / 1000}s`)),
      timeoutMs
    );

    unsubscribe = subscribe((state) => {
      if (isUsableSyncState(state)) finish();
    });
    if (settled) {
      unsubscribe();
      return;
    }

    // Close the gap between the pre-subscribe check and listener installation.
    // startClient() can reach PREPARED before its returned promise settles.
    if (isUsableSyncState(getCurrentState())) finish();
  });
}
