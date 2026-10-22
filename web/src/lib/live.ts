'use client'

import { useEffect, useState } from 'react'
import { snapshotIn } from './api'
import type { Snapshot } from './types'

export type Live = { snapshot?: Snapshot; connected: boolean }

export function useLive(): Live {
  const [live, setLive] = useState<Live>({ connected: false })
  useEffect(() => {
    let source: EventSource | undefined
    let retry: ReturnType<typeof setTimeout> | undefined
    let closed = false
    const open = () => {
      source = new EventSource('/api/stream')
      source.onmessage = (e: MessageEvent<string>) => {
        const snapshot = snapshotIn(e.data)
        if (snapshot) setLive({ snapshot, connected: true })
      }
      source.onerror = () => {
        source?.close()
        setLive((l) => ({ ...l, connected: false }))
        if (!closed) retry = setTimeout(open, 2000)
      }
    }
    open()
    return () => {
      closed = true
      clearTimeout(retry)
      source?.close()
    }
  }, [])
  return live
}
