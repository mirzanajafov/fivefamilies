import { relay, toController, unreachable } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function POST(req: Request) {
  try {
    return relay(await toController('/api/heal', { method: 'POST', admin: true, req }))
  } catch {
    return unreachable()
  }
}
