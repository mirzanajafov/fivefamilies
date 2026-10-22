export function ago(at: string, now: number): string {
  const s = Math.max(0, Math.round((now - Date.parse(at)) / 1000))
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  return `${Math.floor(m / 60)}h ago`
}

export function rate(v: number): string {
  return v >= 100 ? v.toFixed(0) : v.toFixed(1)
}

export function wholeRate(v: number): string {
  return Math.round(v).toLocaleString('en-US')
}

export function millis(ns: number): string {
  return Math.round(ns / 1e6).toLocaleString('en-US')
}

export function ms(ns: number): string {
  return ns >= 10e6 ? `${millis(ns)} ms` : `${(ns / 1e6).toFixed(1)} ms`
}
