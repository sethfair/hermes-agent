import { expect, test } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { channelBuildRequest } from '../scripts/msix-shared.mjs'

const commit = 'a'.repeat(40)
const request = (token, sequence = 65537) => ({ schema: 1, buildId: 'b'.repeat(32), channel: `smoke-${token}`,
  repository: 'NousResearch/hermes-agent', publicBase: 'https://releases.example.test', commit,
  sourceVersion: '1.2.3', sequence, version: `0.0.${sequence}`,
  windowsVersion: `0.${Math.floor(sequence / 65536)}.${sequence % 65536}.0`, bundleEnv: {},
  identity: { token, displayName: 'Smoke Channel', appId: `com.example.preview-${token}`,
    appNamePascal: `Smoke${token}`, artifactNamePascal: `Artifact${token}`,
    msixAppIdWithOrg: `Example.Smoke${token}`, cliName: `smoke-${token}`, windowsExecutableName: `smoke-${token}` } })

// Each validation starts Python and imports the release modules (~90ms on a dev
// machine, several times that on a loaded 4-vCPU runner). Re-admitting a request
// that already passed must not pay that again, and an invalid request must still
// be refused every time.
test.skipIf(process.platform === 'win32')('an already-admitted channel request is not re-validated by a new Python child', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'channel-validation-'))
  const log = path.join(temp, 'spawns'), shim = path.join(temp, 'python')
  fs.writeFileSync(shim, `#!/bin/sh\necho x >> '${log}'\nexec '${process.env.HERMES_PYTHON || 'python'}' "$@"\n`, { mode: 0o755 })
  const env = (value) => ({ ...process.env, HERMES_PYTHON: shim, HERMES_DESKTOP_VARIANT: 'bundled',
    HERMES_BUILD_COMMIT: '', HERMES_PAYLOAD_TAG: '', HERMES_PAYLOAD_VERSION: '', _HERMES_CHANNEL_REQUEST_JSON: JSON.stringify(value) })
  const spawns = () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').trim().split('\n').length : 0)
  try {
    const valid = request('0123456789abcdef')
    const first = channelBuildRequest(env(valid))
    expect(first).toMatchObject({ commit, version: valid.version })
    expect(spawns()).toBe(1)
    expect(channelBuildRequest(env(valid))).toEqual(first)
    expect(spawns()).toBe(1)
    const invalid = { ...valid, windowsVersion: '1.2.3.0' }
    for (let attempt = 1; attempt <= 2; attempt++) {
      expect(() => channelBuildRequest(env(invalid))).toThrow()
      expect(spawns()).toBe(1 + attempt)
    }
  } finally { fs.rmSync(temp, { recursive: true, force: true }) }
})
