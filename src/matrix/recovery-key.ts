// matrix-js-sdk does not currently expose recovery-key helpers from its package
// root. Keep this internal import isolated so SDK changes only affect this file.
export { decodeRecoveryKey } from "matrix-js-sdk/lib/crypto-api/index.js";
