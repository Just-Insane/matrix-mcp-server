import assert from "node:assert/strict";
import test from "node:test";
import {
  resolveDeviceId,
  type DeviceIdFetch,
} from "../src/matrix/device-id.js";

function response(
  body: unknown,
  { ok = true, status = 200 }: { ok?: boolean; status?: number } = {}
) {
  return {
    ok,
    status,
    async json() {
      return body;
    },
  };
}

test("uses an explicitly configured device ID without calling whoami", async () => {
  let called = false;
  const fetchFn: DeviceIdFetch = async () => {
    called = true;
    return response({});
  };

  const deviceId = await resolveDeviceId({
    configuredDeviceId: " DEVICE123 ",
    homeserverUrl: "https://matrix.example.com",
    userId: "@alice:example.com",
    accessToken: "secret",
    fetchFn,
  });

  assert.equal(deviceId, "DEVICE123");
  assert.equal(called, false);
});

test("discovers the access-token device from whoami", async () => {
  const fetchFn: DeviceIdFetch = async (url, init) => {
    assert.equal(
      url,
      "https://matrix.example.com/_matrix/client/v3/account/whoami"
    );
    assert.equal(init?.headers?.Authorization, "Bearer secret");
    return response({
      user_id: "@alice:example.com",
      device_id: "DEVICE123",
    });
  };

  const deviceId = await resolveDeviceId({
    homeserverUrl: "https://matrix.example.com/",
    userId: "@alice:example.com",
    accessToken: "secret",
    fetchFn,
  });

  assert.equal(deviceId, "DEVICE123");
});

test("rejects a whoami response without a device ID", async () => {
  await assert.rejects(
    resolveDeviceId({
      homeserverUrl: "https://matrix.example.com",
      userId: "@alice:example.com",
      accessToken: "secret",
      fetchFn: async () => response({ user_id: "@alice:example.com" }),
    }),
    /set MATRIX_DEVICE_ID/
  );
});

test("rejects invalid and mismatched access tokens before crypto setup", async () => {
  await assert.rejects(
    resolveDeviceId({
      homeserverUrl: "https://matrix.example.com",
      userId: "@alice:example.com",
      accessToken: "secret",
      fetchFn: async () =>
        response(
          { errcode: "M_UNKNOWN_TOKEN" },
          { ok: false, status: 401 }
        ),
    }),
    /HTTP 401 \(M_UNKNOWN_TOKEN\)/
  );

  await assert.rejects(
    resolveDeviceId({
      homeserverUrl: "https://matrix.example.com",
      userId: "@alice:example.com",
      accessToken: "secret",
      fetchFn: async () =>
        response({ user_id: "@mallory:example.com", device_id: "DEVICE123" }),
    }),
    /belongs to @mallory:example.com/
  );
});
