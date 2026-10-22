import type { Snapshot } from './types'

export function errorIn(body: unknown): string | undefined {
  return typeof body === 'object' && body !== null && 'error' in body && typeof body.error === 'string' ? body.error : undefined
}

export function adminIn(body: unknown): boolean {
  return typeof body === 'object' && body !== null && 'admin' in body && body.admin === true
}

function isSnapshot(v: unknown): v is Snapshot {
  return (
    typeof v === 'object' &&
    v !== null &&
    'nodes' in v &&
    Array.isArray(v.nodes) &&
    'events' in v &&
    Array.isArray(v.events) &&
    'windows' in v &&
    Array.isArray(v.windows) &&
    'load' in v &&
    typeof v.load === 'object' &&
    v.load !== null &&
    'limits' in v &&
    typeof v.limits === 'object' &&
    v.limits !== null
  )
}

export function snapshotIn(text: string): Snapshot | undefined {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    return undefined
  }
  return isSnapshot(parsed) ? parsed : undefined
}
