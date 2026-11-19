import https from "node:https";

/** Keep Node's normal CA trust and hostname verification for credential-bearing Matrix requests. */
export function createMatrixHttpsAgent() {
  return new https.Agent({ rejectUnauthorized: true });
}
