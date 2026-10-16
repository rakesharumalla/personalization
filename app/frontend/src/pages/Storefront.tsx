import { useEffect, useMemo, useState } from 'react'
import {
  Alert, Box, Button, Card, CardActionArea, CardContent, CardMedia, Chip, CircularProgress, Divider, FormControlLabel,
  Grid, InputAdornment, MenuItem, Skeleton, Stack, Switch, TextField, ToggleButton, ToggleButtonGroup, Tooltip,
  Typography,
} from '@mui/material'
import SearchIcon from '@mui/icons-material/Search'
import BoltIcon from '@mui/icons-material/Bolt'
import PersonAddIcon from '@mui/icons-material/PersonAdd'
import RestartAltIcon from '@mui/icons-material/RestartAlt'
import ShoppingCartIcon from '@mui/icons-material/ShoppingCart'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api, money, pretty, type Product, type Rec, type RecResponse, type Shopper } from '../api'

const newSession = () => crypto.randomUUID().replace(/-/g, '')

function LatencyBadge({ r }: { r?: RecResponse }) {
  if (!r) return null
  const t = r.timings
  const fallback = !r.served_by?.startsWith('model_serving')
  return (
    <Tooltip
      title={
        <Box>
          <div>Lakebase feature + session lookup: {t.feature_lookup_ms} ms</div>
          <div>Model Serving ({r.model_variant}): {t.model_ms} ms</div>
          <div>Lakebase catalog join: {t.catalog_ms} ms</div>
          <div>Served by: {r.served_by}</div>
          {r.error && <div>Serving error: {r.error}</div>}
        </Box>
      }
    >
      <Chip
        icon={<BoltIcon />}
        size="small"
        color={t.total_ms < 100 ? 'success' : t.total_ms < 300 ? 'warning' : 'error'}
        label={`${Math.round(t.total_ms)} ms${fallback ? ' · fallback' : ''}`}
      />
    </Tooltip>
  )
}

function WhyThis({ rec, userId, uf, sessionTitles }: { rec: Rec; userId: string; uf: Record<string, unknown>; sessionTitles: string[] }) {
  const { data, isLoading } = useQuery({
    queryKey: ['explain', userId, rec.product_id],
    queryFn: () =>
      api.post<{ explanation: string | null; cached: boolean }>('/api/explain', {
        user_id: userId, product: rec, user_features: uf, session_titles: sessionTitles,
      }),
    staleTime: Infinity,
  })
  if (isLoading) return <Skeleton variant="text" width="90%" />
  if (!data?.explanation) return null
  return (
    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontStyle: 'italic', mt: 0.5 }}>
      ✨ {data.explanation}
    </Typography>
  )
}

function ProductTile({ p, onClick, footer, badge }: { p: Partial<Product>; onClick?: () => void; footer?: React.ReactNode; badge?: string }) {
  return (
    <Card sx={{ height: '100%', position: 'relative' }}>
      <CardActionArea onClick={onClick} sx={{ height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'stretch' }}>
        <CardMedia component="img" image={p.image_url} sx={{ aspectRatio: '1 / 1', objectFit: 'cover' }} />
        {badge && <Chip size="small" label={badge} sx={{ position: 'absolute', top: 8, left: 8, bgcolor: 'rgba(255,255,255,.9)' }} />}
        <CardContent sx={{ p: 1.25, flex: 1 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, lineHeight: 1.2 }} noWrap title={p.title}>
            {p.title}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            {pretty(p.category)} · {money(p.price)}
          </Typography>
          {footer}
        </CardContent>
      </CardActionArea>
    </Card>
  )
}

function RecRail({ title, subtitle, r, userId, onPick, explain }: {
  title: string; subtitle?: string; r?: RecResponse; userId: string; onPick: (rec: Rec, r: RecResponse) => void; explain: boolean
}) {
  const sessionTitles = useMemo(() => [], [])
  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: "center", mb: 1 }}>
        <Typography variant="h6">{title}</Typography>
        <LatencyBadge r={r} />
        {r && <Chip size="small" variant="outlined" label={r.model_variant} />}
      </Stack>
      {subtitle && <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>{subtitle}</Typography>}
      {!r ? (
        <Grid container spacing={1.5}>
          {Array.from({ length: 5 }).map((_, i) => (
            <Grid key={i} size={{ xs: 6, md: 2.4 }}><Skeleton variant="rounded" height={260} /></Grid>
          ))}
        </Grid>
      ) : (
        <Grid container spacing={1.5}>
          {r.recommendations.slice(0, 10).map((rec) => (
            <Grid key={rec.product_id} size={{ xs: 6, md: 2.4 }}>
              <ProductTile
                p={rec}
                badge={pretty(rec.source)}
                onClick={() => onPick(rec, r)}
                footer={explain ? <WhyThis rec={rec} userId={userId} uf={r.user_features} sessionTitles={sessionTitles} /> : null}
              />
            </Grid>
          ))}
        </Grid>
      )}
    </Box>
  )
}

export default function Storefront() {
  const qc = useQueryClient()
  const [userId, setUserId] = useState<string>('')
  const [sessionId, setSessionId] = useState(newSession)
  const [variant, setVariant] = useState<'champion' | 'challenger'>('champion')
  const [compare, setCompare] = useState(true)
  const [explain, setExplain] = useState(true)
  const [category, setCategory] = useState<string>('')
  const [q, setQ] = useState('')
  const [viewing, setViewing] = useState<Partial<Product> | null>(null)
  const [tick, setTick] = useState(0)

  const shoppers = useQuery({ queryKey: ['shoppers'], queryFn: () => api.get<Shopper[]>('/api/shoppers?limit=4') })
  useEffect(() => {
    if (!userId && shoppers.data?.length) setUserId(shoppers.data[0].user_id)
  }, [shoppers.data, userId])

  const profile = useQuery({
    queryKey: ['shopper', userId],
    queryFn: () => api.get<Record<string, unknown>>(`/api/shoppers/${userId}`),
    enabled: !!userId,
  })
  const cats = useQuery({ queryKey: ['cats'], queryFn: () => api.get<{ category: string }[]>('/api/categories') })
  const products = useQuery({
    queryKey: ['products', category, q],
    queryFn: () => api.get<Product[]>(`/api/products?limit=18${category ? `&category=${category}` : ''}${q ? `&q=${encodeURIComponent(q)}` : ''}`),
  })
  const recs = useQuery({
    queryKey: ['recs', userId, sessionId, variant, compare, tick],
    queryFn: () =>
      api.post<{ personalized: RecResponse; baseline?: RecResponse }>('/api/recs', {
        user_id: userId, session_id: sessionId, k: 10, variant, compare,
      }),
    enabled: !!userId,
    placeholderData: (prev) => prev,
  })
  const session = useQuery({
    queryKey: ['session', sessionId, tick],
    queryFn: () => api.get<{ event_type: string; title: string; category: string }[]>(`/api/session/${sessionId}`),
  })
  const similar = useQuery({
    queryKey: ['similar', viewing?.product_id],
    queryFn: () => api.get<Product[]>(`/api/similar/${viewing!.product_id}`),
    enabled: !!viewing?.product_id,
  })

  const send = async (event_type: string, p: Partial<Product>, extra: Record<string, unknown> = {}) => {
    await api.post('/api/events', { user_id: userId, session_id: sessionId, event_type, product_id: p.product_id, ...extra })
    setTick((t) => t + 1)
  }
  const view = (p: Partial<Product>, extra: Record<string, unknown> = {}) => {
    setViewing(p)
    send('view', p, extra)
  }
  const pickRec = (rec: Rec, r: RecResponse) =>
    view(rec, { request_id: r.request_id, position: rec.position, model_variant: r.model_variant })

  const switchShopper = (id: string) => {
    setUserId(id)
    setSessionId(newSession())
    setViewing(null)
  }
  const createShopper = async () => {
    const s = await api.post<Shopper>('/api/shoppers/new', {})
    qc.setQueryData(['shopper', s.user_id], s)
    switchShopper(s.user_id)
  }

  const p = profile.data || {}
  const cold = !p.has_history
  const personalized = recs.data?.personalized
  const isNew = userId.startsWith('NEW-')

  return (
    <Grid container spacing={3}>
      <Grid size={{ xs: 12, md: 3 }}>
        <Stack spacing={2} sx={{ position: 'sticky', top: 88 }}>
          <Card>
            <CardContent>
              <Typography variant="overline">Shopper</Typography>
              <TextField
                select fullWidth size="small" value={shoppers.data?.some((s) => s.user_id === userId) ? userId : ''}
                onChange={(e) => switchShopper(e.target.value)} label={isNew ? `${userId} (new)` : 'Pick a shopper'}
              >
                {(shoppers.data || []).map((s) => (
                  <MenuItem key={s.user_id} value={s.user_id}>
                    {s.user_id} · {pretty(s.segment)} · {pretty(s.top_category)}
                  </MenuItem>
                ))}
              </TextField>
              <Button startIcon={<PersonAddIcon />} size="small" sx={{ mt: 1 }} onClick={createShopper}>
                New shopper (cold start)
              </Button>
              <Divider sx={{ my: 1.5 }} />
              <Stack sx={{ flexWrap: "wrap", gap: 0.75 }} direction="row">
                <Chip size="small" color={cold ? 'warning' : 'primary'} label={cold ? 'cold start' : pretty(String(p.segment))} />
                {!!p.loyalty_tier && <Chip size="small" variant="outlined" label={`tier: ${p.loyalty_tier}`} />}
                {!!p.country && <Chip size="small" variant="outlined" label={String(p.country)} />}
                {!!p.top_category && <Chip size="small" variant="outlined" label={`likes ${pretty(String(p.top_category))}`} />}
                {!!p.top_brand && <Chip size="small" variant="outlined" label={`brand ${p.top_brand}`} />}
                {p.n_orders_90d !== undefined && <Chip size="small" variant="outlined" label={`${p.n_orders_90d} orders / 90d`} />}
              </Stack>
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
                Features read live from Lakebase <code>serving.user_features_online</code> (synced from <code>gold.user_features</code>).
              </Typography>
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="overline">Model</Typography>
              <ToggleButtonGroup size="small" exclusive fullWidth value={variant} onChange={(_, v) => v && setVariant(v)}>
                <ToggleButton value="champion">@champion</ToggleButton>
                <ToggleButton value="challenger">@challenger</ToggleButton>
              </ToggleButtonGroup>
              <FormControlLabel sx={{ mt: 1 }} control={<Switch checked={compare} onChange={(e) => setCompare(e.target.checked)} />}
                label={<Typography variant="body2">Show what a non-personalized user sees</Typography>} />
              <FormControlLabel control={<Switch checked={explain} onChange={(e) => setExplain(e.target.checked)} />}
                label={<Typography variant="body2">GenAI "why this?"</Typography>} />
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <Stack sx={{ alignItems: "center", justifyContent: "space-between" }} direction="row">
                <Typography variant="overline">Live session (Lakebase session_state)</Typography>
                <Tooltip title="Start a new session">
                  <Button size="small" onClick={() => { setSessionId(newSession()); setViewing(null) }}><RestartAltIcon fontSize="small" /></Button>
                </Tooltip>
              </Stack>
              {(session.data || []).length === 0 && (
                <Typography variant="body2" color="text.secondary">Click products — every click is written to Lakebase and the next recommendation uses it.</Typography>
              )}
              <Stack spacing={0.5}>
                {(session.data || []).slice(0, 8).map((e, i) => (
                  <Typography key={i} variant="body2" noWrap>
                    <Chip size="small" label={e.event_type} sx={{ mr: 1, height: 20 }} />
                    {e.title}
                  </Typography>
                ))}
              </Stack>
            </CardContent>
          </Card>
        </Stack>
      </Grid>

      <Grid size={{ xs: 12, md: 9 }}>
        <Stack spacing={3}>
          {personalized?.error && (
            <Alert severity="info">
              Model Serving unavailable ({personalized.error.split(':')[0]}) — served from Lakebase <code>precomputed_recs</code> fallback.
            </Alert>
          )}

          {viewing && (
            <Card sx={{ p: 2 }}>
              <Grid sx={{ alignItems: "center" }} container spacing={2}>
                <Grid size={{ xs: 4, md: 2 }}>
                  <Box component="img" src={viewing.image_url} sx={{ width: '100%', borderRadius: 2 }} />
                </Grid>
                <Grid size={{ xs: 8, md: 4 }}>
                  <Typography variant="overline">Now viewing</Typography>
                  <Typography variant="h6">{viewing.title}</Typography>
                  <Typography color="text.secondary">{pretty(viewing.category)} · {viewing.brand} · {money(viewing.price)}</Typography>
                  <Stack direction="row" spacing={1} sx={{ mt: 1 }}>
                    <Button variant="outlined" size="small" startIcon={<ShoppingCartIcon />} onClick={() => send('add_to_cart', viewing)}>Add to cart</Button>
                    <Button variant="contained" size="small" onClick={() => send('purchase', viewing)}>Buy now</Button>
                  </Stack>
                </Grid>
                <Grid size={{ xs: 12, md: 6 }}>
                  <Typography variant="overline">Similar to what you just viewed · Vector Search</Typography>
                  {similar.isLoading ? <CircularProgress size={20} /> : (
                    <Stack direction="row" spacing={1} sx={{ overflowX: 'auto' }}>
                      {(similar.data || []).slice(0, 5).map((s) => (
                        <Box key={s.product_id} sx={{ width: 92, flexShrink: 0, cursor: 'pointer' }} onClick={() => view(s)}>
                          <Box component="img" src={s.image_url} sx={{ width: 92, borderRadius: 1 }} />
                          <Typography variant="caption" sx={{ display: 'block', lineHeight: 1.1 }} noWrap title={s.title}>{s.title}</Typography>
                        </Box>
                      ))}
                    </Stack>
                  )}
                </Grid>
              </Grid>
            </Card>
          )}

          <RecRail
            title={cold ? 'Picked for you (cold start: session + semantic signals)' : 'Recommended for you'}
            subtitle="Two-stage recommender on Model Serving: ALS + Vector-embedding candidates → LightGBM ranker over Lakebase features + live session."
            r={personalized} userId={userId} onPick={pickRec} explain={explain}
          />
          {compare && (
            <RecRail title="What a non-personalized shopper sees" subtitle="Popularity baseline (the control arm of the A/B test)."
              r={recs.data?.baseline} userId={userId} onPick={pickRec} explain={false} />
          )}

          <Box>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} sx={{ alignItems: { md: 'center' }, mb: 1.5 }}>
              <Typography variant="h6" sx={{ flexShrink: 0 }}>Browse the catalog</Typography>
              <TextField size="small" placeholder="Search products" value={q} onChange={(e) => setQ(e.target.value)}
                slotProps={{ input: { startAdornment: <InputAdornment position="start"><SearchIcon fontSize="small" /></InputAdornment> } }} />
              <Stack sx={{ flexWrap: "wrap", gap: 0.75 }} direction="row">
                <Chip label="all" size="small" color={!category ? 'primary' : 'default'} onClick={() => setCategory('')} />
                {(cats.data || []).map((c) => (
                  <Chip key={c.category} size="small" label={pretty(c.category)} color={category === c.category ? 'primary' : 'default'}
                    onClick={() => setCategory(c.category)} />
                ))}
              </Stack>
            </Stack>
            <Grid container spacing={1.5}>
              {(products.data || []).map((prod) => (
                <Grid key={prod.product_id} size={{ xs: 6, sm: 4, md: 2 }}>
                  <ProductTile p={prod} onClick={() => view(prod)} />
                </Grid>
              ))}
            </Grid>
          </Box>
        </Stack>
      </Grid>
    </Grid>
  )
}
