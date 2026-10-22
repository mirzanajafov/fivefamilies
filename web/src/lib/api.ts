export function errorIn(body: unknown): string | undefined {
  return typeof body === 'object' && body !== null && 'error' in body && typeof body.error === 'string' ? body.error : undefined
}
