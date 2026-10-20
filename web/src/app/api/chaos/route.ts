import { forward } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function POST(req: Request) {
  const body = await req.text()
  if (body.length > 1024) {
    return Response.json({ error: 'request too large' }, { status: 413 })
  }
  return forward('/api/chaos', { method: 'POST', body, admin: true, req, headers: { 'Content-Type': 'application/json' } })
}
