import { relay, toController, unreachable } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    return relay(await toController('/api/snapshot'))
  } catch {
    return unreachable()
  }
}
