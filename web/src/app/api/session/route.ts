import { cookies } from 'next/headers'
import { ADMIN_COOKIE, CONTROLLER, adminToken, readBody } from '@/lib/server'

export const dynamic = 'force-dynamic'

async function valid(token: string): Promise<boolean> {
  try {
    const res = await fetch(`${CONTROLLER}/api/admin`, { headers: { Authorization: `Bearer ${token}` }, cache: 'no-store' })
    return res.ok
  } catch {
    return false
  }
}

export async function GET() {
  const token = await adminToken()
  return Response.json({ admin: token ? await valid(token) : false })
}

function tokenIn(body: string): string | undefined {
  try {
    const { token } = JSON.parse(body) as { token?: unknown }
    return typeof token === 'string' ? token : undefined
  } catch {
    return undefined
  }
}

export async function POST(req: Request) {
  const body = await readBody(req, 1024)
  if (body === undefined) {
    return Response.json({ error: 'request too large' }, { status: 413 })
  }
  const token = tokenIn(body)
  if (!token || token.length > 512 || !(await valid(token))) {
    return Response.json({ error: 'that token does not open the admin controls' }, { status: 401 })
  }
  ;(await cookies()).set(ADMIN_COOKIE, token, {
    httpOnly: true,
    sameSite: 'strict',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: 8 * 3600,
  })
  return Response.json({ admin: true })
}

export async function DELETE() {
  ;(await cookies()).delete(ADMIN_COOKIE)
  return Response.json({ admin: false })
}
