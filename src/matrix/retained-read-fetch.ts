import type fetch from "node-fetch";

/** The SDK's automatic key backup downloader also uses this transport. */
export function retainedReadFetch(fetchFn: typeof fetch): typeof fetch {
  return async (input, init) => {
    const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
    const pathname = decodeURIComponent(url.pathname);
    if (pathname.includes("/room_keys/") || pathname.endsWith("/room_keys")) {
      throw new Error("Retained Matrix read mode: key backup restore and upload are disabled");
    }
    return fetchFn(input, { ...init, redirect: "error" });
  };
}
