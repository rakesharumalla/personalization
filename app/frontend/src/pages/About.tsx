import { useState } from 'react'
import { Box, Button, Card, CardContent, Skeleton, Stack, Tab, Tabs, Typography } from '@mui/material'
import OpenInNewIcon from '@mui/icons-material/OpenInNew'
import { useQuery } from '@tanstack/react-query'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { api } from '../api'

type About = { about: string; decisions: string; spec: string; links: Record<string, string> }

const LINK_LABELS: Record<string, string> = {
  catalog: 'Unity Catalog', lineage: 'Lineage (gold.user_features)', model: 'Model in UC', serving: 'Serving endpoint',
  lakebase: 'Lakebase project', genie: 'Genie space', vector_index: 'Vector Search index',
}

function Md({ children }: { children: string }) {
  return (
    <Box sx={{ '& table': { borderCollapse: 'collapse', my: 1 }, '& td, & th': { border: '1px solid #ddd', p: 0.75, fontSize: 14 },
      '& code': { bgcolor: '#f3f3f3', px: 0.5, borderRadius: 0.5 }, '& pre': { bgcolor: '#f6f6f6', p: 1.5, overflow: 'auto', borderRadius: 1 } }}>
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{children}</ReactMarkdown>
    </Box>
  )
}

export default function AboutPage() {
  const [tab, setTab] = useState(0)
  const { data, isLoading } = useQuery({ queryKey: ['about'], queryFn: () => api.get<About>('/api/about') })
  return (
    <Stack spacing={2}>
      <Typography variant="h5">About This Build</Typography>
      <Stack sx={{ flexWrap: "wrap", gap: 1 }} direction="row">
        {Object.entries(LINK_LABELS).map(([k, label]) => data?.links?.[k] && (
          <Button key={k} size="small" variant="outlined" endIcon={<OpenInNewIcon fontSize="small" />} href={data.links[k]} target="_blank">{label}</Button>
        ))}
      </Stack>
      <Tabs value={tab} onChange={(_, v) => setTab(v)}>
        <Tab label="The four questions" />
        <Tab label="Architecture" />
        <Tab label="Build log & trade-offs" />
        <Tab label="Spec" />
      </Tabs>
      <Card>
        <CardContent>
          {isLoading && <Skeleton variant="rounded" height={300} />}
          {data && tab === 0 && <Md>{data.about}</Md>}
          {data && tab === 1 && (
            <Box>
              <Box component="img" src="/architecture.svg" alt="architecture" sx={{ width: '100%', maxWidth: 1200 }} />
            </Box>
          )}
          {data && tab === 2 && <Md>{data.decisions}</Md>}
          {data && tab === 3 && <Md>{data.spec}</Md>}
        </CardContent>
      </Card>
    </Stack>
  )
}
