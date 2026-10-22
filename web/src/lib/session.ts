'use client'

import { useEffect, useState } from 'react'

export function adminIn(body: unknown): boolean {
  return typeof body === 'object' && body !== null && 'admin' in body && body.admin === true
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
