import { useRef, useState } from 'react'
import {
  Accordion, AccordionDetails, AccordionSummary, Alert, Box, Button, Card, CardContent, Chip, CircularProgress, Stack,
  Tab, Table, TableBody, TableCell, TableHead, TableRow, Tabs, TextField, Typography,
} from '@mui/material'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import BuildIcon from '@mui/icons-material/Build'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { api, streamAgent, type AgentEvent } from '../api'

type GenieAnswer = {
  conversation_id: string; text?: string; sql?: string; description?: string; columns: string[]; rows: unknown[][];
  suggested_questions?: string[]; error?: string
}

const GENIE_SAMPLES = [
  'Which product categories have the highest recommendation CTR this week?',
  'How do cold-start users convert vs. returning users?',
  'Show me the lift of the challenger model over champion by country',
  'What is the conversion lift of personalized recommendations over the popularity baseline by segment in the last 7 days?',
  'What were daily sessions, conversion rate and revenue over the last 14 days?',
]
const AGENT_SAMPLES = [
  'Which segment should we target with a 10% electronics campaign, and what lift should we expect?',
  'Find gift ideas for a home cook and tell me how the home_kitchen category performs with personalization.',
  'Compare champion vs challenger and recommend whether to promote the challenger.',
]

function ResultTable({ columns, rows }: { columns: string[]; rows: unknown[][] }) {
  if (!columns?.length) return null
  return (
    <Box sx={{ maxHeight: 320, overflow: 'auto', border: '1px solid #eee', borderRadius: 1, mt: 1 }}>
      <Table size="small" stickyHeader>
        <TableHead><TableRow>{columns.map((c) => <TableCell key={c}>{c}</TableCell>)}</TableRow></TableHead>
        <TableBody>
          {rows.slice(0, 50).map((r, i) => (
            <TableRow key={i}>{r.map((v, j) => <TableCell key={j}>{fmtCell(v)}</TableCell>)}</TableRow>
          ))}
        </TableBody>
      </Table>
    </Box>
  )
}

function fmtCell(v: unknown) {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  if (typeof v === 'string' && v.trim() !== '' && !Number.isNaN(n) && v.includes('.')) return n.toFixed(4)
  return String(v)
}

function GenieChat() {
  const [q, setQ] = useState('')
  const [conv, setConv] = useState<string | undefined>()
  const [turns, setTurns] = useState<{ q: string; a?: GenieAnswer; loading?: boolean }[]>([])
  const ask = async (question: string) => {
    if (!question.trim()) return
    setQ('')
    setTurns((t) => [...t, { q: question, loading: true }])
    try {
      const a = await api.post<GenieAnswer>('/api/genie', { question, conversation_id: conv })
      setConv(a.conversation_id)
      setTurns((t) => t.map((x, i) => (i === t.length - 1 ? { q: question, a } : x)))
    } catch (e) {
      setTurns((t) => t.map((x, i) => (i === t.length - 1 ? { q: question, a: { error: String(e), columns: [], rows: [], conversation_id: '' } } : x)))
    }
  }
  return (
    <Stack spacing={2}>
      <Stack sx={{ flexWrap: "wrap", gap: 1 }} direction="row">
        {GENIE_SAMPLES.map((s) => <Chip key={s} label={s} onClick={() => ask(s)} variant="outlined" />)}
      </Stack>
      {turns.map((t, i) => (
        <Card key={i}>
          <CardContent>
            <Typography variant="subtitle2" color="primary">{t.q}</Typography>
            {t.loading && <Stack direction="row" spacing={1} sx={{ alignItems: "center", mt: 1 }}><CircularProgress size={16} /><Typography variant="body2">Genie is writing SQL…</Typography></Stack>}
            {t.a?.error && <Alert severity="error" sx={{ mt: 1 }}>{t.a.error}</Alert>}
            {t.a?.text && <Box sx={{ mt: 1 }}><ReactMarkdown remarkPlugins={[remarkGfm]}>{t.a.text}</ReactMarkdown></Box>}
            {t.a?.description && <Typography variant="body2" sx={{ mt: 1 }}>{t.a.description}</Typography>}
            {t.a && <ResultTable columns={t.a.columns} rows={t.a.rows} />}
            {t.a?.sql && (
              <Accordion disableGutters elevation={0} sx={{ mt: 1, border: '1px solid #eee' }}>
                <AccordionSummary expandIcon={<ExpandMoreIcon />}><Typography variant="caption">Generated SQL</Typography></AccordionSummary>
                <AccordionDetails><Box component="pre" sx={{ m: 0, fontSize: 12, whiteSpace: 'pre-wrap' }}>{t.a.sql}</Box></AccordionDetails>
              </Accordion>
            )}
          </CardContent>
        </Card>
      ))}
      <Stack direction="row" spacing={1}>
        <TextField fullWidth size="small" placeholder="Ask a question about recommendation performance…" value={q}
          onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && ask(q)} />
        <Button variant="contained" onClick={() => ask(q)}>Ask Genie</Button>
        {conv && <Button onClick={() => { setConv(undefined); setTurns([]) }}>New chat</Button>}
      </Stack>
    </Stack>
  )
}

type Turn = { q: string; events: AgentEvent[]; answer?: string; running: boolean }

function Copilot() {
  const [q, setQ] = useState('')
  const [turns, setTurns] = useState<Turn[]>([])
  const history = useRef<{ role: string; content: string }[]>([])
  const ask = async (question: string) => {
    if (!question.trim()) return
    setQ('')
    setTurns((t) => [...t, { q: question, events: [], running: true }])
    const update = (fn: (t: Turn) => Turn) => setTurns((ts) => ts.map((x, i) => (i === ts.length - 1 ? fn(x) : x)))
    let answer = ''
    try {
      await streamAgent({ question, history: history.current }, (e) => {
        if (e.type === 'answer') { answer = e.content; update((t) => ({ ...t, answer: e.content })) }
        else if (e.type === 'error') update((t) => ({ ...t, answer: `⚠️ ${e.content}` }))
        else if (e.type !== 'done') update((t) => ({ ...t, events: [...t.events, e] }))
      })
    } catch (e) {
      update((t) => ({ ...t, answer: `⚠️ ${String(e)}` }))
    }
    update((t) => ({ ...t, running: false }))
    history.current = [...history.current, { role: 'user', content: question }, { role: 'assistant', content: answer }].slice(-8)
  }
  return (
    <Stack spacing={2}>
      <Stack sx={{ flexWrap: "wrap", gap: 1 }} direction="row">
        {AGENT_SAMPLES.map((s) => <Chip key={s} label={s} onClick={() => ask(s)} variant="outlined" />)}
      </Stack>
      {turns.map((t, i) => (
        <Card key={i}>
          <CardContent>
            <Typography variant="subtitle2" color="primary">{t.q}</Typography>
            <Stack spacing={0.75} sx={{ my: 1 }}>
              {t.events.map((e, j) =>
                e.type === 'tool_call' ? (
                  <Stack sx={{ alignItems: "center" }} key={j} direction="row" spacing={1}>
                    <BuildIcon fontSize="small" color="action" />
                    <Typography variant="body2"><b>{e.name}</b>({Object.entries(e.arguments).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(', ')})</Typography>
                  </Stack>
                ) : e.type === 'tool_result' ? (
                  <Accordion key={j} disableGutters elevation={0} sx={{ border: '1px solid #eee', ml: 3.5 }}>
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Typography variant="caption">{e.ok ? '✓' : '✗'} {e.name} result · {e.elapsed_ms} ms</Typography>
                    </AccordionSummary>
                    <AccordionDetails>
                      <Box component="pre" sx={{ m: 0, fontSize: 11, whiteSpace: 'pre-wrap', maxHeight: 240, overflow: 'auto' }}>
                        {JSON.stringify(e.result, null, 1)}
                      </Box>
                    </AccordionDetails>
                  </Accordion>
                ) : null,
              )}
            </Stack>
            {t.running && !t.answer && <Stack sx={{ alignItems: "center" }} direction="row" spacing={1}><CircularProgress size={16} /><Typography variant="body2">Thinking…</Typography></Stack>}
            {t.answer && <ReactMarkdown remarkPlugins={[remarkGfm]}>{t.answer}</ReactMarkdown>}
          </CardContent>
        </Card>
      ))}
      <Stack direction="row" spacing={1}>
        <TextField fullWidth size="small" placeholder="Ask the merchandiser copilot — campaigns, products, performance…" value={q}
          onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && ask(q)} />
        <Button variant="contained" onClick={() => ask(q)}>Ask copilot</Button>
      </Stack>
    </Stack>
  )
}

export default function Ask() {
  const [tab, setTab] = useState(0)
  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h5">Ask the Data</Typography>
        <Typography color="text.secondary">
          Genie answers quantitative questions over governed gold tables. The Merchandiser Copilot is a tool-calling agent that combines Genie,
          a campaign simulator (UC function <code>gold.simulate_campaign</code>) and Vector Search over the catalog.
        </Typography>
      </Box>
      <Tabs value={tab} onChange={(_, v) => setTab(v)}>
        <Tab label="Genie — Personalization Insights" />
        <Tab label="Merchandiser Copilot" />
      </Tabs>
      <Box hidden={tab !== 0}><GenieChat /></Box>
      <Box hidden={tab !== 1}><Copilot /></Box>
    </Stack>
  )
}
