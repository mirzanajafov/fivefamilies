import { forward } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function POST(req: Request) {
  return forward('/api/heal', { method: 'POST', admin: true, req })
}
