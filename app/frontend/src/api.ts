export type Rec = {
  product_id: string
  title?: string
  description?: string
  category?: string
  brand?: string
  price?: number
  image_url?: string
  price_tier?: string
  score: number
  source: string
  position: number
  signals?: Record<string, number | boolean>
}

export type RecResponse = {
  request_id: string
  user_id: string
  session_items: string[]
  served_by: string
  model_variant: string
  timings: { feature_lookup_ms: number; model_ms: number; catalog_ms: number; total_ms: number }
  error?: string | null
  user_features: Record<string, unknown>
  recommendations: Rec[]
}

export type Product = {
  product_id: string
  title: string
  category: string
  brand: string
  price: number
  image_url: string
  price_tier?: string
  description?: string
}

export type Shopper = {
  user_id: string
  segment: string
  country?: string
  loyalty_tier?: string
  top_category?: string
  top_brand?: string
  n_orders_90d?: number
  n_sessions_30d?: number
  has_history?: boolean
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...(init?.headers || {}) },
  })
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`)
  return r.json() as Promise<T>
}

export const api = {
  get: <T,>(path: string) => req<T>(path),
  post: <T,>(path: string, body: unknown) => req<T>(path, { method: 'POST', body: JSON.stringify(body) }),
}

export type AgentEvent =
  | { type: 'tool_call'; id: string; name: string; arguments: Record<string, unknown> }
  | { type: 'tool_result'; id: string; name: string; ok: boolean; result: unknown; elapsed_ms: number }
  | { type: 'answer'; content: string; steps?: number; elapsed_ms?: number }
  | { type: 'error'; content: string }
  | { type: 'done' }

/** POST + read a text/event-stream response, calling onEvent for every `data:` frame. */
export async function streamAgent(body: unknown, onEvent: (e: AgentEvent) => void) {
  const r = await fetch('/api/agent', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok || !r.body) throw new Error(`${r.status}`)
  const reader = r.body.getReader()
  const dec = new TextDecoder()
  let buf = ''
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    buf += dec.decode(value, { stream: true })
    let i
    while ((i = buf.indexOf('\n\n')) >= 0) {
      const frame = buf.slice(0, i)
      buf = buf.slice(i + 2)
      const line = frame.split('\n').find((l) => l.startsWith('data: '))
      if (line) onEvent(JSON.parse(line.slice(6)))
    }
  }
}

export const pct = (v: unknown, d = 1) => (typeof v === 'number' ? `${(v * 100).toFixed(d)}%` : '—')
export const money = (v: unknown) => (typeof v === 'number' ? `$${v.toFixed(2)}` : '—')
export const pretty = (s?: string | null) => (s || '').replace(/_/g, ' ')
