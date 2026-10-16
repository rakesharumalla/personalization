import { AppBar, Box, Chip, Tab, Tabs, Toolbar, Tooltip, Typography } from '@mui/material'
import { Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import Storefront from './pages/Storefront'
import Console from './pages/Console'
import Ask from './pages/Ask'
import About from './pages/About'
import { api } from './api'

const PAGES = [
  { path: '/storefront', label: 'Storefront Simulator' },
  { path: '/console', label: 'Business Console' },
  { path: '/ask', label: 'Ask the Data' },
  { path: '/about', label: 'About This Build' },
]

function Health() {
  const { data } = useQuery({
    queryKey: ['health'],
    queryFn: () => api.get<Record<string, string>>('/api/health'),
    refetchInterval: 30000,
  })
  if (!data) return null
  const ok = (v?: string) => v === 'ok' || v === 'READY'
  return (
    <Box sx={{ display: 'flex', gap: 1 }}>
      <Tooltip title={`Lakebase: ${data.lakebase}`}>
        <Chip size="small" label="Lakebase" color={ok(data.lakebase) ? 'success' : 'warning'} variant="outlined" />
      </Tooltip>
      <Tooltip title={`Model Serving: ${data.model_serving}`}>
        <Chip size="small" label="Model Serving" color={ok(data.model_serving) ? 'success' : 'warning'} variant="outlined" />
      </Tooltip>
      <Tooltip title={`Genie space: ${data.genie_space_id || 'not found'}`}>
        <Chip size="small" label="Genie" color={data.genie_space_id && !String(data.genie_space_id).startsWith('error') ? 'success' : 'warning'} variant="outlined" />
      </Tooltip>
    </Box>
  )
}

export default function App() {
  const loc = useLocation()
  const nav = useNavigate()
  const current = PAGES.findIndex((p) => loc.pathname.startsWith(p.path))
  return (
    <Box sx={{ minHeight: '100vh' }}>
      <AppBar position="sticky" color="secondary" elevation={0}>
        <Toolbar sx={{ gap: 3 }}>
          <Box>
            <Typography variant="h6" sx={{ lineHeight: 1.1 }}>
              Acme Retail · Real-Time Personalization
            </Typography>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
              Lakeflow · Unity Catalog · Lakebase · Model Serving · Genie — on Databricks
            </Typography>
          </Box>
          <Tabs
            value={current < 0 ? 0 : current}
            onChange={(_, i) => nav(PAGES[i].path)}
            textColor="inherit"
            slotProps={{ indicator: { style: { backgroundColor: '#FF3621', height: 3 } } }}
            sx={{ flex: 1 }}
          >
            {PAGES.map((p) => (
              <Tab key={p.path} label={p.label} />
            ))}
          </Tabs>
          <Health />
        </Toolbar>
      </AppBar>
      <Box sx={{ p: 3, maxWidth: 1500, mx: 'auto' }}>
        <Routes>
          <Route path="/" element={<Navigate to="/storefront" replace />} />
          <Route path="/storefront" element={<Storefront />} />
          <Route path="/console" element={<Console />} />
          <Route path="/ask" element={<Ask />} />
          <Route path="/about" element={<About />} />
          <Route path="*" element={<Navigate to="/storefront" replace />} />
        </Routes>
      </Box>
    </Box>
  )
}
