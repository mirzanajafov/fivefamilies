import { forward, readBody } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function POST(req: Request) {
  const body = await readBody(req)
  if (body instanceof Response) return body
  return forward('/api/chaos', { method: 'POST', body, admin: true, req, headers: { 'Content-Type': 'application/json' } })
}
