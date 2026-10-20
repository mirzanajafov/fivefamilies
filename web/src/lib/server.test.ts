import { afterEach, expect, it, vi } from 'vitest'
import { forward, readBody, tokenIn } from './server'

vi.mock('server-only', () => ({}))
vi.mock('next/headers', () => ({ cookies: () => Promise.resolve({ get: () => ({ value: 'admin-token' }) }) }))

afterEach(() => {
  vi.unstubAllGlobals()
})

it('passes the controller answer through with its status', async () => {
  vi.stubGlobal('fetch', () => Promise.resolve(Response.json({ error: 'busy' }, { status: 429 })))
  const res = await forward('/api/chaos')
  expect(res.status).toBe(429)
  expect(await res.json()).toEqual({ error: 'busy' })
})

it('passes the visitor address on, and the admin token only on admin calls', async () => {
  const seen: Headers[] = []
  vi.stubGlobal('fetch', (_url: string, init: RequestInit) => {
    seen.push(new Headers(init.headers))
    return Promise.resolve(Response.json({}))
  })
  const req = new Request('http://localhost/api/heal', { headers: { 'x-forwarded-for': '203.0.113.7' } })
  await forward('/api/heal', { admin: true, req })
  await forward('/api/chaos', { req })
  expect(seen[0]?.get('Authorization')).toBe('Bearer admin-token')
  expect(seen[0]?.get('X-Forwarded-For')).toBe('203.0.113.7')
  expect(seen[1]?.get('Authorization')).toBeNull()
  expect(seen[1]?.get('X-Forwarded-For')).toBe('203.0.113.7')
})

it('answers 502 when the controller cannot be reached', async () => {
  vi.stubGlobal('fetch', () => Promise.reject(new TypeError('fetch failed')))
  const res = await forward('/api/heal')
  expect(res.status).toBe(502)
  expect(await res.json()).toEqual({ error: 'the controller is unreachable' })
})

it('answers 502 when the controller body breaks off', async () => {
  const body = new ReadableStream({
    start(c) {
      c.error(new Error('connection reset'))
    },
  })
  vi.stubGlobal('fetch', () => Promise.resolve(new Response(body)))
  const res = await forward('/api/chaos')
  expect(res.status).toBe(502)
  expect(await res.json()).toEqual({ error: 'the controller stopped in the middle of its answer' })
})

it('reads a body within the limit', async () => {
  expect(await readBody(new Request('http://localhost/api/chaos', { method: 'POST', body: '{"action":"kill"}' }))).toBe('{"action":"kill"}')
})

it('answers 413 to a declared length over the limit without reading the body', async () => {
  const req = new Request('http://localhost/api/chaos', { method: 'POST', body: 'x', headers: { 'content-length': '5000' } })
  const res = await readBody(req)
  expect(res instanceof Response && res.status).toBe(413)
  expect(req.bodyUsed).toBe(false)
})

it('stops reading an endless body once it passes the limit', async () => {
  let pulled = 0
  let cancelled = false
  const body = new ReadableStream<Uint8Array<ArrayBuffer>>({
    pull(c) {
      pulled++
      c.enqueue(new Uint8Array(512))
    },
    cancel() {
      cancelled = true
    },
  })
  const res = await readBody({ headers: new Headers(), body })
  expect(res instanceof Response && res.status).toBe(413)
  expect(cancelled).toBe(true)
  expect(pulled).toBeLessThan(10)
})

it('answers 400 when the body breaks off', async () => {
  const body = new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(c) {
      c.error(new Error('connection reset'))
    },
  })
  const res = await readBody({ headers: new Headers(), body })
  expect(res instanceof Response && res.status).toBe(400)
})

it('takes the admin token only when it is a string', () => {
  expect(tokenIn('{"token":"secret"}')).toBe('secret')
  expect(tokenIn('{"token":["secret"]}')).toBeUndefined()
  expect(tokenIn('null')).toBeUndefined()
  expect(tokenIn('not json')).toBeUndefined()
})
