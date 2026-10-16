import 'server-only'
import { cookies } from 'next/headers'

export const CONTROLLER = process.env.CONTROLLER_URL ?? 'http://127.0.0.1:3159'
export const ADMIN_COOKIE = 'ff_admin'

export async function adminToken(): Promise<string | undefined> {
  return (await cookies()).get(ADMIN_COOKIE)?.value
}

export async function toController(path: string, init: RequestInit & { admin?: boolean; req?: Request } = {}): Promise<Response> {
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

export async function relay(res: Response): Promise<Response> {
  return new Response(await res.text(), {
    status: res.status,
    headers: { 'Content-Type': res.headers.get('Content-Type') ?? 'application/json' },
  })
}

export function unreachable(): Response {
  return Response.json({ error: 'the controller is unreachable' }, { status: 502 })
}
