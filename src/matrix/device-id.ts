export interface DeviceIdResponse {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

export type DeviceIdFetch = (
  input: string,
  init?: { headers?: Record<string, string> }
) => Promise<DeviceIdResponse>;

interface ResolveDeviceIdOptions {
  configuredDeviceId?: string;
  homeserverUrl: string;
  userId: string;
  accessToken: string;
  fetchFn: DeviceIdFetch;
}

interface WhoAmIResponse {
  user_id?: unknown;
  device_id?: unknown;
  errcode?: unknown;
}

export async function resolveDeviceId({
  configuredDeviceId,
  homeserverUrl,
  userId,
  accessToken,
  fetchFn,
}: ResolveDeviceIdOptions): Promise<string> {
  const explicitDeviceId = configuredDeviceId?.trim();
  if (explicitDeviceId) {
    return explicitDeviceId;
  }

  const response = await fetchFn(
    `${homeserverUrl.replace(/\/+$/, "")}/_matrix/client/v3/account/whoami`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  const body = (await response.json()) as WhoAmIResponse;

  if (!response.ok) {
    const errcode =
      typeof body.errcode === "string" ? ` (${body.errcode})` : "";
    throw new Error(
      `Matrix whoami failed with HTTP ${response.status}${errcode}`
    );
  }

  if (body.user_id !== userId) {
    throw new Error(
      `Matrix access token belongs to ${String(body.user_id)}, expected ${userId}`
    );
  }

  if (typeof body.device_id !== "string" || !body.device_id.trim()) {
    throw new Error(
      "Matrix whoami did not return a device_id; set MATRIX_DEVICE_ID to the device associated with MATRIX_ACCESS_TOKEN"
    );
  }

  return body.device_id.trim();
}
