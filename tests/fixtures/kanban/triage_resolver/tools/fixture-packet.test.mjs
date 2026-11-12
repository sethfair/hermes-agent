import assert from 'node:assert/strict'
import { createHash, createPrivateKey, createPublicKey, sign, verify } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const root = join(here, '..', 'wri2385.triage-resolver.v2')
const magic = Buffer.from('5752493233383502', 'hex')
const U = (value) => ({ kind: 'uint', value: BigInt(value) })
const R = (...values) => ({ kind: 'record', values })

function sized(payload) {
  const length = Buffer.alloc(4)
  length.writeUInt32BE(payload.length)
  return Buffer.concat([length, payload])
}

function encode(value) {
  if (value === null) return Buffer.from([0])
  if (value === false) return Buffer.from([1])
  if (value === true) return Buffer.from([2])
  if (value?.kind === 'uint') {
    assert(value.value >= 0n)
    let hex = value.value.toString(16)
    if (hex === '0') hex = ''
    if (hex.length % 2) hex = `0${hex}`
    return Buffer.concat([Buffer.from([3]), sized(Buffer.from(hex, 'hex'))])
  }
  if (value?.kind === 'record') {
    const count = Buffer.alloc(4)
    count.writeUInt32BE(value.values.length)
    return Buffer.concat([Buffer.from([8]), count, ...value.values.map(encode)])
  }
  if (typeof value === 'string') return Buffer.concat([Buffer.from([5]), sized(Buffer.from(value, 'utf8'))])
  if (Buffer.isBuffer(value)) return Buffer.concat([Buffer.from([6]), sized(value)])
  if (Array.isArray(value)) {
    const count = Buffer.alloc(4)
    count.writeUInt32BE(value.length)
    return Buffer.concat([Buffer.from([7]), count, ...value.map(encode)])
  }
  if (value && value.constructor === Object) {
    const entries = Object.entries(value).sort(([a], [b]) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
    const count = Buffer.alloc(4)
    count.writeUInt32BE(entries.length)
    return Buffer.concat([
      Buffer.from([9]),
      count,
      ...entries.flatMap(([key, item]) => [encode(key), encode(item)]),
    ])
  }
  throw new TypeError(`unsupported fixture value ${typeof value}`)
}

function preimage(domain, ...members) {
  return Buffer.concat([magic, encode(R(domain, ...members))])
}

function digest(domain, ...members) {
  return createHash('sha256').update(preimage(domain, ...members)).digest('hex')
}

function fromB64url(value) {
  return Buffer.from(value, 'base64url')
}

async function fixture(name) {
  return JSON.parse(await readFile(join(root, name), 'utf8'))
}

test('Node independently reconstructs canonical v2 codec, assertion and transport fixtures', async () => {
  const vectors = await fixture('codec-vectors.json')
  const assertion = await fixture('office-principal-assertion.json')
  const signRequest = await fixture('office-sign-request.json')
  const signResponse = await fixture('office-sign-response.json')
  const invokeRequest = await fixture('core-invoke-request.json')
  const invokeResponse = await fixture('core-invoke-response-completed.json')
  const invokeUnknown = await fixture('core-invoke-response-unknown.json')
  const requestFrame = await readFile(join(root, 'resolver-request-frame.bin'))
  const responseFrame = await readFile(join(root, 'resolver-response-frame.bin'))

  const sourceBytes = Buffer.from(vectors.machine_id.source_bytes_hex, 'hex')
  assert.equal(
    preimage('hermes/machine-id/v1', vectors.machine_id.source_kind, sourceBytes).toString('hex'),
    vectors.machine_id.preimage_hex,
  )
  assert.equal(digest('hermes/machine-id/v1', vectors.machine_id.source_kind, sourceBytes), vectors.machine_id.digest)

  const board = R(invokeRequest.machine_id, invokeRequest.board_id, invokeRequest.board)
  const requestDigest = digest(
    'wri2385/triage-request/v2',
    'wri2385.triage-resolver.v2',
    assertion.operation,
    assertion.request_id,
    board,
    't_fixture_root',
  )
  assert.equal(requestDigest, assertion.request_digest)

  const claimMembers = [
    assertion.assertion_version,
    assertion.issuer,
    assertion.algorithm,
    assertion.kid,
    assertion.principal,
    assertion.session_id,
    U(assertion.session_epoch),
    U(assertion.principal_epoch),
    assertion.audience,
    assertion.machine_id,
    assertion.board_id,
    assertion.operation,
    assertion.request_id,
    assertion.request_digest,
    assertion.jti,
    U(assertion.issued_at),
    U(assertion.expires_at),
  ]
  const claimBytes = preimage('wri2385/office-principal-assertion/v2', ...claimMembers)
  assert.equal(claimBytes.toString('hex'), vectors.assertion.preimage_hex)
  assert.deepEqual(claimBytes, fromB64url(signRequest.claim_bytes))
  assert.equal(digest('wri2385/office-sign-claim/v2', ...claimMembers), signRequest.claim_digest)

  const seed = Buffer.from(vectors.assertion.private_key_seed_hex, 'hex')
  const privateDer = Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), seed])
  const privateKey = createPrivateKey({ key: privateDer, format: 'der', type: 'pkcs8' })
  const publicKey = createPublicKey(privateKey)
  const publicDer = publicKey.export({ format: 'der', type: 'spki' })
  assert.equal(publicDer.subarray(-32).toString('hex'), vectors.assertion.public_key_hex)
  const signature = fromB64url(assertion.signature)
  assert.equal(signature.toString('hex'), vectors.assertion.signature_hex)
  assert.deepEqual(sign(null, claimBytes, privateKey), signature)
  assert.equal(verify(null, claimBytes, publicKey, signature), true)
  assert.equal(signResponse.signature, assertion.signature)

  assert.equal(requestFrame.readUInt32BE(0), requestFrame.length - 4)
  assert.equal(invokeRequest.request_length, requestFrame.length)
  assert.deepEqual(fromB64url(invokeRequest.request_bytes), requestFrame)
  assert.equal(
    digest('wri2385/core-invoke-request/v2', U(requestFrame.length), requestFrame),
    invokeRequest.request_sha256,
  )

  assert.equal(responseFrame.readUInt32BE(0), responseFrame.length - 4)
  assert.equal(invokeResponse.response_length, responseFrame.length)
  assert.deepEqual(fromB64url(invokeResponse.response_bytes), responseFrame)
  assert.equal(
    digest('wri2385/core-invoke-response/v2', U(responseFrame.length), responseFrame),
    invokeResponse.response_sha256,
  )
  assert.equal(invokeUnknown.result, 'unknown')
  assert.equal(invokeUnknown.response_length, null)
  assert.equal(invokeUnknown.response_sha256, null)
  assert.equal(invokeUnknown.response_bytes, null)
})
