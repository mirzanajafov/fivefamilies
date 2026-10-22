'use client'

import { useEffect, useState } from 'react'
import { adminIn } from './api'

export function useNow(every: number): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), every)
    return () => clearInterval(t)
  }, [every])
  return now
}

export function useAdmin(): [boolean | undefined, (admin: boolean) => void] {
  const [admin, setAdmin] = useState<boolean>()
  useEffect(() => {
    fetch('/api/session')
      .then((r) => r.json())
      .then((body: unknown) => setAdmin(adminIn(body)))
      .catch(() => setAdmin(false))
  }, [])
  return [admin, setAdmin]
}
