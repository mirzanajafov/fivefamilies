import 'server-only'
import { cookies } from 'next/headers'

export const CONTROLLER = process.env.CONTROLLER_URL ?? 'http://127.0.0.1:3159'
export const ADMIN_COOKIE = 'ff_admin'

export async function adminToken(): Promise<string | undefined> {
  return (await cookies()).get(ADMIN_COOKIE)?.value
}

const maxBody = 1024

export async function readBody(req: Pick<Request, 'headers' | 'body'>): Promise<string | Response> {
  const tooLarge = Response.json({ error: 'request too large' }, { status: 413 })
  if (Number(req.headers.get('content-length')) > maxBody) return tooLarge
  if (!req.body) return ''
  const reader = req.body.getReader()
  const chunks: Uint8Array[] = []
  let size = 0
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) return Buffer.concat(chunks).toString('utf8')
      size += value.length
      if (size > maxBody) {
        await reader.cancel()
        return tooLarge
      }
      chunks.push(value)
    }
  } catch {
    return Response.json({ error: 'the request body was cut off' }, { status: 400 })
  }
}

export function tokenIn(body: string): string | undefined {
  let parsed: unknown
  try {
    parsed = JSON.parse(body)
  } catch {
    return undefined
  }
  return typeof parsed === 'object' && parsed !== null && 'token' in parsed && typeof parsed.token === 'string' ? parsed.token : undefined
}

type ControllerInit = RequestInit & { admin?: boolean; req?: Request }

async function toController(path: string, init: ControllerInit): Promise<Response> {
  const headers = new Headers(init.headers)
  if (init.admin) {
    const token = await adminToken()
    if (token) headers.set('Authorization', `Bearer ${token}`)
  }
  const xff = init.req?.headers.get('x-forwarded-for')
  if (xff) headers.set('X-Forwarded-For', xff)
  const { admin: _admin, req: _req, ...rest } = init
  return fetch(`${CONTROLLER}${path}`, { ...rest, headers, cache: 'no-store' })
}

export async function forward(path: string, init: ControllerInit = {}): Promise<Response> {
  let res: Response
  try {
    res = await toController(path, init)
  } catch {
    return Response.json({ error: 'the controller is unreachable' }, { status: 502 })
  }
  try {
    return new Response(await res.text(), {
      status: res.status,
      headers: { 'Content-Type': res.headers.get('Content-Type') ?? 'application/json' },
    })
  } catch {
    return Response.json({ error: 'the controller stopped in the middle of its answer' }, { status: 502 })
  }
}
