import 'server-only'
import { cookies } from 'next/headers'

export const CONTROLLER = process.env.CONTROLLER_URL ?? 'http://127.0.0.1:3159'
export const ADMIN_COOKIE = 'ff_admin'

export async function adminToken(): Promise<string | undefined> {
  return (await cookies()).get(ADMIN_COOKIE)?.value
}

type ControllerInit = RequestInit & { admin?: boolean; req?: Request }

export async function toController(path: string, init: ControllerInit = {}): Promise<Response> {
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
  try {
    const res = await toController(path, init)
    return new Response(await res.text(), {
      status: res.status,
      headers: { 'Content-Type': res.headers.get('Content-Type') ?? 'application/json' },
    })
  } catch {
    return Response.json({ error: 'the controller is unreachable' }, { status: 502 })
  }
}
