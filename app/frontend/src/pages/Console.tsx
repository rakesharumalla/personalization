import { Alert, Box, Card, CardContent, Chip, Grid, Skeleton, Stack, Table, TableBody, TableCell, TableHead, TableRow, Typography } from '@mui/material'
import { useQuery } from '@tanstack/react-query'
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { api, money, pct, pretty } from '../api'

type Row = Record<string, any>
const COLORS: Record<string, string> = { popularity: '#94A3B8', champion: '#FF3621', challenger: '#1B3139', baseline: '#94A3B8', personalized: '#FF3621' }

function useKpi(name: string) {
  return useQuery({ queryKey: ['kpi', name], queryFn: () => api.get<Row[]>(`/api/kpis/${name}`) })
}

function Stat({ label, base, pers, fmt = pct, target }: { label: string; base?: number; pers?: number; fmt?: (v: unknown) => string; target?: string }) {
  const lift = base && pers ? pers / base - 1 : undefined
  return (
    <Card sx={{ height: '100%' }}>
      <CardContent>
        <Typography variant="overline" color="text.secondary">{label}</Typography>
        <Stack sx={{ alignItems: "baseline" }} direction="row" spacing={1}>
          <Typography variant="h4">{fmt(pers)}</Typography>
          {lift !== undefined && <Chip size="small" color={lift >= 0 ? 'success' : 'error'} label={`${lift >= 0 ? '+' : ''}${(lift * 100).toFixed(0)}%`} />}
        </Stack>
        <Typography variant="body2" color="text.secondary">baseline {fmt(base)} {target && `· target ${target}`}</Typography>
      </CardContent>
    </Card>
  )
}

function Panel({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <Card sx={{ height: '100%' }}>
      <CardContent>
        <Typography variant="h6">{title}</Typography>
        {subtitle && <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>{subtitle}</Typography>}
        {children}
      </CardContent>
    </Card>
  )
}

function pivot(rows: Row[] | undefined, key: string, series: string, value: string) {
  const m = new Map<string, Row>()
  for (const r of rows || []) {
    const k = String(r[key])
    m.set(k, { ...(m.get(k) || { [key]: k }), [r[series]]: r[value] })
  }
  return [...m.values()]
}

export default function Console() {
  const summary = useKpi('summary')
  const daily = useKpi('daily')
  const segments = useKpi('segments')
  const categories = useKpi('categories')
  const offline = useKpi('offline_eval')
  const dq = useKpi('data_quality')
  const funnel = useKpi('funnel')
  const live = useQuery({ queryKey: ['live'], queryFn: () => api.get<Row[]>('/api/kpis/live'), refetchInterval: 15000 })
  const pipeline = useQuery({ queryKey: ['pipeline'], queryFn: () => api.get<Row[]>('/api/pipeline'), refetchInterval: 30000 })

  const by = Object.fromEntries((Array.isArray(summary.data) ? summary.data : []).map((r) => [r.model_variant, r]))
  const base = by.popularity || {}
  const pers = by.champion || {}
  const err = [summary, daily].find((q) => q.error)?.error as Error | undefined
  const d = Array.isArray(dq.data) ? dq.data[0] : undefined

  return (
    <Stack spacing={3}>
      <Box>
        <Typography variant="h5">Business Console</Typography>
        <Typography color="text.secondary">
          Simulated A/B test — popularity baseline (control, 50%) vs. personalized champion/challenger (treatment, 25% + 25%).
          Outcomes are drawn from the simulator's hidden shopper preferences, so lift is earned by ranking quality. Source: <code>gold.experiment_results</code>.
        </Typography>
      </Box>
      {err && <Alert severity="warning">KPIs unavailable: {err.message}</Alert>}
      {summary.isLoading ? <Skeleton variant="rounded" height={120} /> : (
        <Grid container spacing={2}>
          <Grid size={{ xs: 6, md: 2.4 }}><Stat label="Recommendation CTR" base={base.ctr} pers={pers.ctr} target="≥ 6%" /></Grid>
          <Grid size={{ xs: 6, md: 2.4 }}><Stat label="Session conversion" base={base.conversion} pers={pers.conversion} fmt={(v) => pct(v, 2)} target="≥ 2.6%" /></Grid>
          <Grid size={{ xs: 6, md: 2.4 }}><Stat label="Cold-start conversion" base={base.cold_conversion} pers={pers.cold_conversion} fmt={(v) => pct(v, 2)} target="≥ 1.4%" /></Grid>
          <Grid size={{ xs: 6, md: 2.4 }}><Stat label="AOV" base={base.aov} pers={pers.aov} fmt={money} target="+5–8%" /></Grid>
          <Grid size={{ xs: 12, md: 2.4 }}><Stat label="Revenue (14d, per arm)" base={base.revenue} pers={(pers.revenue || 0) + (by.challenger?.revenue || 0)} fmt={(v) => (typeof v === 'number' ? `$${Math.round(v).toLocaleString()}` : '—')} /></Grid>
        </Grid>
      )}

      <Grid container spacing={2}>
        <Grid size={{ xs: 12, md: 8 }}>
          <Panel title="Daily recommendation CTR by variant">
            <ResponsiveContainer width="100%" height={260}>
              <LineChart data={pivot(daily.data, 'event_date', 'model_variant', 'ctr')}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                <XAxis dataKey="event_date" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={(v) => pct(v, 0)} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(v) => pct(v)} />
                <Legend />
                {['popularity', 'champion', 'challenger'].map((k) => (
                  <Line key={k} dataKey={k} stroke={COLORS[k]} strokeWidth={2} dot={false} />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </Panel>
        </Grid>
        <Grid size={{ xs: 12, md: 4 }}>
          <Panel title="Champion vs. challenger" subtitle="Offline (temporal holdout) and online (simulated A/B)">
            <Table size="small">
              <TableHead><TableRow><TableCell>Variant</TableCell><TableCell>NDCG@10</TableCell><TableCell>Recall@50</TableCell><TableCell>CTR</TableCell><TableCell>Conv.</TableCell></TableRow></TableHead>
              <TableBody>
                {(Array.isArray(offline.data) ? offline.data : []).map((r) => (
                  <TableRow key={r.model_variant}>
                    <TableCell><Chip size="small" label={r.model_variant} sx={{ bgcolor: COLORS[r.model_variant], color: '#fff' }} /></TableCell>
                    <TableCell>{r.ndcg_at_10?.toFixed(3)}</TableCell>
                    <TableCell>{r.recall_at_50?.toFixed(3)}</TableCell>
                    <TableCell>{pct(by[r.model_variant]?.ctr)}</TableCell>
                    <TableCell>{pct(by[r.model_variant]?.conversion, 2)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Panel>
        </Grid>
        <Grid size={{ xs: 12, md: 6 }}>
          <Panel title="Conversion by segment" subtitle="Baseline vs. personalized">
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={pivot(segments.data, 'segment', 'arm', 'conversion')}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                <XAxis dataKey="segment" tickFormatter={pretty} tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={(v) => pct(v, 1)} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(v) => pct(v, 2)} />
                <Legend />
                <Bar dataKey="baseline" fill={COLORS.baseline} />
                <Bar dataKey="personalized" fill={COLORS.personalized} />
              </BarChart>
            </ResponsiveContainer>
          </Panel>
        </Grid>
        <Grid size={{ xs: 12, md: 6 }}>
          <Panel title="Recommendation CTR by category">
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={pivot(categories.data, 'category', 'arm', 'ctr')}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                <XAxis dataKey="category" tickFormatter={pretty} tick={{ fontSize: 10 }} interval={0} angle={-25} textAnchor="end" height={50} />
                <YAxis tickFormatter={(v) => pct(v, 0)} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(v) => pct(v)} />
                <Legend />
                <Bar dataKey="baseline" fill={COLORS.baseline} />
                <Bar dataKey="personalized" fill={COLORS.personalized} />
              </BarChart>
            </ResponsiveContainer>
          </Panel>
        </Grid>
        <Grid size={{ xs: 12, md: 6 }}>
          <Panel title="Organic funnel (Lakeflow gold)" subtitle="gold.session_kpis_daily — sessions and conversion over the last 28 days">
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={funnel.data && Array.isArray(funnel.data) ? funnel.data : []}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                <XAxis dataKey="session_date" tick={{ fontSize: 11 }} />
                <YAxis yAxisId="l" tick={{ fontSize: 11 }} />
                <YAxis yAxisId="r" orientation="right" tickFormatter={(v) => pct(v, 1)} tick={{ fontSize: 11 }} />
                <Tooltip />
                <Legend />
                <Line yAxisId="l" dataKey="sessions" stroke="#1B3139" dot={false} />
                <Line yAxisId="r" dataKey="conversion_rate" stroke="#FF3621" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </Panel>
        </Grid>
        <Grid size={{ xs: 12, md: 3 }}>
          <Panel title="Data quality" subtitle="Lakeflow expectations (gold.pipeline_health)">
            {d ? (
              <Table size="small">
                <TableBody>
                  <TableRow><TableCell>Bronze events</TableCell><TableCell align="right">{d.bronze_events?.toLocaleString()}</TableCell></TableRow>
                  <TableRow><TableCell>Silver events</TableCell><TableCell align="right">{d.silver_events?.toLocaleString()}</TableCell></TableRow>
                  <TableRow><TableCell>Dropped: null user_id</TableCell><TableCell align="right">{d.null_user_id}</TableCell></TableRow>
                  <TableRow><TableCell>Dropped: bad event_type</TableCell><TableCell align="right">{d.invalid_event_type}</TableCell></TableRow>
                  <TableRow><TableCell>Deduplicated</TableCell><TableCell align="right">{d.duplicate_events}</TableCell></TableRow>
                  <TableRow><TableCell>Quarantined</TableCell><TableCell align="right">{d.quarantined_events}</TableCell></TableRow>
                  <TableRow><TableCell>Orders dropped (price ≤ 0)</TableCell><TableCell align="right">{d.invalid_orders}</TableCell></TableRow>
                  <TableRow><TableCell>Drop rate</TableCell><TableCell align="right">{pct(1 - d.silver_events / Math.max(1, d.bronze_events), 2)}</TableCell></TableRow>
                </TableBody>
              </Table>
            ) : <Skeleton variant="rounded" height={160} />}
          </Panel>
        </Grid>
        <Grid size={{ xs: 12, md: 3 }}>
          <Panel title="Freshness & live serving" subtitle="Pipeline state + app recommendation_log (Lakebase)">
            <Stack spacing={1}>
              {Array.isArray(pipeline.data) && pipeline.data.map((p) => (
                <Typography key={p.pipeline_id} variant="body2">Pipeline: <Chip size="small" label={p.state} /> last update {p.latest_updates?.[0]?.state}</Typography>
              ))}
              {d && <Typography variant="body2">Last event ingested: {String(d.last_ingest_ts).replace('T', ' ').slice(0, 19)} UTC</Typography>}
              {(Array.isArray(live.data) ? live.data : []).filter((r) => r.served_by_kind !== 'event').map((r) => (
                <Typography key={r.served_by_kind} variant="body2">
                  {pretty(r.served_by_kind)}: {r.requests} requests · p50 {Math.round(r.p50_ms || 0)} ms · p95 {Math.round(r.p95_ms || 0)} ms
                </Typography>
              ))}
              {(Array.isArray(live.data) ? live.data : []).filter((r) => r.served_by_kind === 'event').map((r) => (
                <Typography key="ev" variant="body2">Live clicks on recs: {r.clicks} · purchases {r.purchases}</Typography>
              ))}
            </Stack>
          </Panel>
        </Grid>
      </Grid>
    </Stack>
  )
}
