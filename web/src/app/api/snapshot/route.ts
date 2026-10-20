import { forward } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function GET() {
  return forward('/api/snapshot')
}
