import { afterEach, expect, it, vi } from 'vitest'
import { forward } from './server'

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
