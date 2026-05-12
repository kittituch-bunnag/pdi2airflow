import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  ReactFlowProvider,
  useReactFlow,
} from 'reactflow'
import {
  UploadCloud, Database, Hash, AlertTriangle, CheckCircle2, XCircle,
  Zap, Terminal, Circle, Download, X, FileCode2, FileText,
  Settings2, Workflow, Clock, Code2,
} from 'lucide-react'

import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

import PdiNode from './components/PdiNode.jsx'
import TechIcon from './components/TechIcon.jsx'

// ── Markdown components for the Report tab ───────────────────────────

const MD_COMPONENTS = {
  // Detect GitHub-style alert blockquotes: > [!WARNING] / > [!NOTE] etc.
  blockquote({ node, children }) {
    const firstPara = node?.children?.[0]
    const firstText = (firstPara?.children ?? []).map((n) => n.value ?? '').join('')
    const match = firstText.match(/^\[!(WARNING|CAUTION|NOTE|TIP|IMPORTANT)\]$/)
    if (match) {
      return <div className={`md-alert md-alert-${match[1].toLowerCase()}`}>{children}</div>
    }
    return <blockquote className="md-blockquote">{children}</blockquote>
  },
}

const NODE_TYPES = { pdi: PdiNode }

const NODE_KINDS = [
  'start', 'end', 'dummy',
  'transformation', 'subjob', 'sql', 'shell', 'eval', 'waitfor', 'unknown',
]

// ── IR <-> ReactFlow conversion ──────────────────────────────────

function irToFlow(graph) {
  const nodes = graph.nodes.map((n) => ({
    id: n.id,
    type: 'pdi',
    position: { x: n.position?.x ?? 0, y: n.position?.y ?? 0 },
    data: {
      label: n.label,
      kind: n.kind,
      config: n.config,
      steps: n.steps,
      step_edges: n.step_edges,
      source_file: n.source_file,
      unmappedCount: (n.steps || []).filter(
        (s) => !['TableInput', 'TableOutput', 'ExecSQL', 'SelectValues',
                 'FilterRows', 'Constant', 'GetVariable', 'SetVariable',
                 'SystemInfo', 'SetValueConstant', 'JsonOutput', 'Rest',
                 'ScriptValueMod', 'WriteToLog'].includes(s.pdi_type)
      ).length,
    },
  }))
  const edges = graph.edges.map((e, i) => ({
    id: `e_${i}_${e.source}_${e.target}`,
    source: e.source,
    target: e.target,
    label: e.condition !== 'unconditional' ? e.condition : undefined,
    data: { condition: e.condition },
  }))
  return { nodes, edges }
}

function flowToIr(graph, flowNodes, flowEdges) {
  const idToOriginal = new Map(graph.nodes.map((n) => [n.id, n]))
  const nodes = flowNodes.map((fn) => {
    const orig = idToOriginal.get(fn.id) || {}
    return {
      id: fn.id,
      label: fn.data.label,
      kind: fn.data.kind,
      config: fn.data.config || orig.config || {},
      steps: fn.data.steps || orig.steps || [],
      step_edges: fn.data.step_edges || orig.step_edges || [],
      source_file: fn.data.source_file || orig.source_file || null,
      position: { x: fn.position.x, y: fn.position.y },
    }
  })
  const edges = flowEdges.map((e) => ({
    source: e.source,
    target: e.target,
    condition: e.data?.condition || 'unconditional',
  }))
  return { ...graph, nodes, edges }
}

// ── Editor (inner — uses useReactFlow) ───────────────────────────

function Editor() {
  const [graph, setGraph] = useState(null)
  const [flowNodes, setFlowNodes] = useState([])
  const [flowEdges, setFlowEdges] = useState([])
  const [selected, setSelected] = useState(null)
  const [isDragging, setIsDragging] = useState(false)
  const [parserErrors, setParserErrors] = useState([])
  const [kettleProps, setKettleProps] = useState(null)
  const [repositories, setRepositories] = useState(null)
  const [genResult, setGenResult] = useState(null)
  const [genError, setGenError] = useState(null)
  const [isGenerating, setIsGenerating] = useState(false)
  const fileRef = useRef(null)
  const rf = useReactFlow()

  const onNodesChange = useCallback(
    (changes) => setFlowNodes((nds) => applyNodeChanges(changes, nds)), []
  )
  const onEdgesChange = useCallback(
    (changes) => setFlowEdges((eds) => applyEdgeChanges(changes, eds)), []
  )
  const onConnect = useCallback(
    (params) => setFlowEdges((eds) => addEdge({ ...params, data: { condition: 'unconditional' } }, eds)), []
  )
  const onSelectionChange = useCallback(({ nodes }) => {
    setSelected(nodes && nodes.length === 1 ? nodes[0] : null)
  }, [])

  const upload = useCallback(async (files) => {
    const fd = new FormData()
    for (const f of files) fd.append('files', f)
    setGraph(null); setSelected(null); setGenResult(null); setGenError(null)
    setKettleProps(null); setRepositories(null)
    try {
      const res = await fetch('/api/upload', { method: 'POST', body: fd })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        alert(`Upload failed: ${err.error || res.statusText}`)
        return
      }
      const data = await res.json()
      setGraph(data.graph)
      setParserErrors(data.parser_errors || [])
      setKettleProps(data.kettle_properties || null)
      setRepositories(data.repositories || null)
      const { nodes, edges } = irToFlow(data.graph)
      setFlowNodes(nodes)
      setFlowEdges(edges)
      setTimeout(() => rf.fitView({ padding: 0.2 }), 50)
    } catch (e) {
      alert(`Upload error: ${e.message}`)
    }
  }, [rf])

  const onFilePick = useCallback((e) => {
    const files = Array.from(e.target.files || [])
    if (files.length) upload(files)
  }, [upload])

  const onDrop = useCallback((e) => {
    e.preventDefault()
    setIsDragging(false)
    const files = Array.from(e.dataTransfer.files || [])
    if (files.length) upload(files)
  }, [upload])

  const addTask = useCallback((kind) => {
    const id = `task_${Date.now()}`
    setFlowNodes((nds) => [...nds, {
      id, type: 'pdi',
      position: { x: 200, y: 200 },
      data: { label: `new_${kind}`, kind, config: {}, steps: [], step_edges: [], source_file: null, unmappedCount: 0 },
    }])
  }, [])

  const deleteSelected = useCallback(() => {
    const ids = flowNodes.filter((n) => n.selected).map((n) => n.id)
    if (!ids.length) return
    setFlowNodes((nds) => nds.filter((n) => !ids.includes(n.id)))
    setFlowEdges((eds) => eds.filter((e) => !ids.includes(e.source) && !ids.includes(e.target)))
    setSelected(null)
  }, [flowNodes])

  const updateSelected = useCallback((patch) => {
    if (!selected) return
    setFlowNodes((nds) => nds.map((n) => n.id === selected.id ? { ...n, data: { ...n.data, ...patch } } : n))
    setSelected((s) => s ? { ...s, data: { ...s.data, ...patch } } : s)
  }, [selected])

  const generate = useCallback(async () => {
    if (!graph) return
    setIsGenerating(true); setGenResult(null); setGenError(null)
    const updatedGraph = flowToIr(graph, flowNodes, flowEdges)
    try {
      const res = await fetch('/api/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ graph: updatedGraph }),
      })
      const body = await res.json()
      if (!res.ok) setGenError(body)
      else setGenResult(body)
    } catch (e) {
      setGenError({ error: e.message, issues: [] })
    } finally {
      setIsGenerating(false)
    }
  }, [graph, flowNodes, flowEdges])

  const liveIssues = useMemo(() => {
    if (!graph) return []
    const issues = []
    for (const f of graph.missing_files) issues.push(`Missing file: ${f}`)
    for (const c of graph.missing_connections) issues.push(`Unmapped connection: ${c}`)
    for (const v of graph.missing_variables) issues.push(`Undefined variable: \${${v}}`)
    for (const t of graph.unmapped_step_types) issues.push(`Unmapped step type: ${t}`)
    return issues
  }, [graph])

  useEffect(() => {
    const onKey = (e) => {
      if ((e.key === 'Delete' || e.key === 'Backspace') &&
          document.activeElement?.tagName !== 'INPUT' &&
          document.activeElement?.tagName !== 'TEXTAREA') {
        deleteSelected()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [deleteSelected])

  return (
    <>
      {/* ── LEFT PANE — source files + metadata ─────────────── */}
      <aside className="pane">
        <div className="pane-header">
          <div className="pane-title">Source Files</div>
        </div>

        <div
          className={`dropzone ${isDragging ? 'dragging' : ''}`}
          onClick={() => fileRef.current?.click()}
          onDragOver={(e) => { e.preventDefault(); setIsDragging(true) }}
          onDragLeave={() => setIsDragging(false)}
          onDrop={onDrop}
        >
          <UploadCloud size={22} className="dz-icon" />
          <div className="dz-main">Drop PDI files here</div>
          <div className="dz-sub">or click to browse</div>
          <div className="dz-types">.ktr · .kjb · .kdb · .zip · .properties</div>
        </div>
        <input
          ref={fileRef}
          type="file"
          multiple
          accept=".zip,.ktr,.kjb,.kdb,.xml,.properties"
          onChange={onFilePick}
        />

        {graph && (
          <>
            {/* DAG info */}
            <div className="section-label">
              <FileCode2 size={11} />
              Workflow
            </div>
            <div className="dag-card">
              <div className="dag-id">{graph.dag_id}</div>
              {graph.source_kjb && <div className="dag-source">{graph.source_kjb}</div>}
            </div>

            {/* Project config */}
            {(kettleProps || repositories) && (
              <>
                <div className="section-label">
                  <Settings2 size={11} />
                  Project Config
                </div>
                <div className="config-card">
                  {kettleProps && (
                    <div className="config-stat">
                      <strong>{kettleProps.user_variable_count}</strong>{' '}
                      user var{kettleProps.user_variable_count !== 1 ? 's' : ''}
                    </div>
                  )}
                  {repositories && (
                    <div className="config-stat">
                      <strong>{repositories.file_repositories.length}</strong>{' '}
                      file repo{repositories.file_repositories.length !== 1 ? 's' : ''}
                    </div>
                  )}
                  {repositories?.database_repositories.length > 0 && (
                    <div className="config-stat">
                      <strong>{repositories.database_repositories.length}</strong> db repo
                      {repositories.database_repositories.length !== 1 ? 's' : ''}
                    </div>
                  )}
                </div>
              </>
            )}

            {/* Connections */}
            <div className="section-label">
              <Database size={11} />
              Connections
              {graph.connections.length > 0 && (
                <span className="pill">{graph.connections.length}</span>
              )}
            </div>
            {graph.connections.length === 0 ? (
              <div className="empty-list">No connections found</div>
            ) : (
              <div className="connections-list">
                {graph.connections.map((c) => (
                  <div key={c.pdi_name} className="conn-item">
                    <div className="conn-icon">
                      <TechIcon type={c.db_type} size={14} />
                    </div>
                    <div className="conn-info">
                      <div className="conn-name">{c.pdi_name}</div>
                      <div className="conn-detail">
                        {c.db_type}{c.suggested_airflow_conn ? ` · ${c.suggested_airflow_conn}` : ''}
                      </div>
                    </div>
                    <span className={`conn-badge ${c.confidence}`}>{c.confidence}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Variables */}
            <div className="section-label">
              <Hash size={11} />
              Variables
              {graph.variables.length > 0 && (
                <span className="pill">{graph.variables.length}</span>
              )}
            </div>
            {graph.variables.length === 0 ? (
              <div className="empty-list">No variables found</div>
            ) : (
              <div className="vars-list">
                {graph.variables.map((v) => (
                  <div key={v.name} className="var-item">
                    <span className="var-name">${'{'}{ v.name }{'}'}</span>
                    <span className="var-usage">used by {v.used_in.length}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Parser warnings */}
            {parserErrors.length > 0 && (
              <>
                <div className="section-label">
                  <AlertTriangle size={11} />
                  Warnings
                  <span className="pill">{parserErrors.length}</span>
                </div>
                <div className="warnings-list">
                  {parserErrors.map((e, i) => (
                    <div key={i} className="warn-item">
                      <AlertTriangle size={11} className="warn-icon" />
                      {e}
                    </div>
                  ))}
                </div>
              </>
            )}
          </>
        )}
      </aside>

      {/* ── CENTER PANE — graph canvas ───────────────────────── */}
      <main className="pane canvas">
        {!graph && (
          <div className="canvas-empty">
            <svg width="220" height="90" viewBox="0 0 220 90" fill="none">
              <rect x="8" y="20" width="58" height="50" rx="8" fill="#21262d" stroke="#30363d" strokeWidth="1.5" />
              <text x="37" y="49" textAnchor="middle" fill="#484f58" fontSize="11" fontFamily="monospace">.kjb</text>
              <line x1="68" y1="45" x2="152" y2="45" stroke="#30363d" strokeWidth="1.5" strokeDasharray="5,3" />
              <polyline points="147,40 152,45 147,50" stroke="#30363d" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round" />
              <rect x="154" y="20" width="58" height="50" rx="8" fill="#21262d" stroke="#30363d" strokeWidth="1.5" />
              <text x="183" y="49" textAnchor="middle" fill="#484f58" fontSize="11" fontFamily="monospace">.py</text>
            </svg>
            <div className="canvas-empty-title">PDI → Airflow</div>
            <div className="canvas-empty-hint">
              Drop a Pentaho job, transformation, or .zip archive in the Source panel to begin.
            </div>
            <div className="canvas-empty-types">
              {['.kjb', '.ktr', '.kdb', '.zip', '.properties'].map((t) => (
                <span key={t} className="canvas-empty-type">{t}</span>
              ))}
            </div>
          </div>
        )}

        {graph && (
          <>
            <div className="canvas-toolbar">
              <button onClick={() => addTask('sql')}>
                <Database size={13} /> SQL
              </button>
              <button onClick={() => addTask('transformation')}>
                <Zap size={13} /> Transform
              </button>
              <button onClick={() => addTask('shell')}>
                <Terminal size={13} /> Shell
              </button>
              <button onClick={() => addTask('dummy')}>
                <Circle size={13} /> Dummy
              </button>
              <div className="tb-spacer" />
              <button className="primary" onClick={generate} disabled={isGenerating}>
                <Zap size={13} />
                {isGenerating ? 'Generating…' : 'Generate DAG'}
              </button>
            </div>

            <ReactFlow
              nodes={flowNodes}
              edges={flowEdges}
              nodeTypes={NODE_TYPES}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              onSelectionChange={onSelectionChange}
              fitView
              proOptions={{ hideAttribution: true }}
            >
              <Background gap={24} size={1} color="#21262d" />
              <Controls />
              <MiniMap
                nodeStrokeColor={() => '#30363d'}
                nodeColor={(n) => {
                  const map = {
                    start: '#3fb950', end: '#f85149', sql: '#2f81f7',
                    transformation: '#e3b341', shell: '#a371f7', subjob: '#ec6cb9',
                    eval: '#f0883e', dummy: '#8b949e', waitfor: '#8b949e', unknown: '#f85149',
                  }
                  return map[n.data?.kind] || '#8b949e'
                }}
                maskColor="rgba(13, 17, 23, 0.75)"
              />
            </ReactFlow>
          </>
        )}
      </main>

      {/* ── RIGHT PANE — inspector ───────────────────────────── */}
      <aside className="pane inspector">
        {liveIssues.length > 0 ? (
          <div className="status-card blocked">
            <div className="status-icon">
              <XCircle size={16} color="var(--danger)" />
            </div>
            <div>
              <div className="status-title">Generation Blocked</div>
              <ul className="status-issues">
                {liveIssues.map((s, i) => <li key={i}>{s}</li>)}
              </ul>
            </div>
          </div>
        ) : graph ? (
          <div className="status-card ready">
            <div className="status-icon">
              <CheckCircle2 size={16} color="var(--success)" />
            </div>
            <div>
              <div className="status-title">Ready</div>
              <div className="status-body">
                All references resolved. Click <span className="kbd">Generate DAG</span>.
              </div>
            </div>
          </div>
        ) : null}

        {selected ? (
          <>
            <div className="inspector-section">
              <div className="inspector-title">Node Details</div>
              <div className="field">
                <label>Task ID</label>
                <input
                  value={selected.id}
                  readOnly
                  style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}
                />
              </div>
              <div className="field">
                <label>Label</label>
                <input
                  value={selected.data.label}
                  onChange={(e) => updateSelected({ label: e.target.value })}
                />
              </div>
              <div className="field">
                <label>Kind</label>
                <select
                  value={selected.data.kind}
                  onChange={(e) => updateSelected({ kind: e.target.value })}
                >
                  {NODE_KINDS.map((k) => (
                    <option key={k} value={k}>{k}</option>
                  ))}
                </select>
              </div>
              {selected.data.source_file && (
                <div className="field">
                  <label>Source File</label>
                  <input
                    value={selected.data.source_file}
                    readOnly
                    style={{ fontFamily: 'var(--font-mono)', fontSize: 10 }}
                  />
                </div>
              )}
            </div>

            {selected.data.kind === 'transformation' && selected.data.steps?.length > 0 && (
              <div className="inspector-section">
                <div className="inspector-title">
                  <Code2 size={11} />
                  Steps
                  <span className="pill">{selected.data.steps.length}</span>
                </div>
                <ul className="steps-list">
                  {selected.data.steps.map((s) => (
                    <li key={s.step_id}>
                      <div className="step-type">{s.pdi_type}</div>
                      <div className="step-name">{s.name}</div>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            <div className="inspector-actions">
              <button
                className="danger"
                onClick={deleteSelected}
                style={{ color: 'var(--danger)' }}
              >
                Delete node
              </button>
            </div>
          </>
        ) : graph ? (
          <div className="inspector-help">
            <strong>Editing Help</strong>
            Drag nodes to reposition · Drag handles to connect<br />
            Click a node to inspect · <span className="kbd">Del</span> to remove<br />
            Use toolbar to add new task nodes
          </div>
        ) : null}
      </aside>

      {/* ── Result modal ─────────────────────────────────────── */}
      {(genResult || genError) && (
        <ResultModal
          result={genResult}
          error={genError}
          onClose={() => { setGenResult(null); setGenError(null) }}
        />
      )}
    </>
  )
}

// ── Result modal ─────────────────────────────────────────────────

function ResultModal({ result, error, onClose }) {
  const [tab, setTab] = useState('dag')

  const download = (filename, content) => {
    const blob = new Blob([content], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = filename; a.click()
    URL.revokeObjectURL(url)
  }

  if (error) {
    return (
      <div className="modal-backdrop" onClick={onClose}>
        <div className="modal" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <XCircle size={18} color="var(--danger)" />
            <h2 style={{ color: 'var(--danger)' }}>Generation Blocked</h2>
            <button className="icon-btn" onClick={onClose} style={{ marginLeft: 'auto' }}>
              <X size={16} />
            </button>
          </div>
          <div className="modal-body">
            <div className="modal-error-body">
              <p>{error.error}</p>
              {error.issues?.length > 0 && (
                <ul>{error.issues.map((i, idx) => <li key={idx}>{i}</li>)}</ul>
              )}
            </div>
          </div>
          <div className="modal-foot">
            <button className="primary" onClick={onClose}>
              <X size={13} /> Close
            </button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <CheckCircle2 size={18} color="var(--success)" />
          <h2>Generated</h2>
          <span className="modal-filename">{result.dag_filename}</span>
          <button className="icon-btn" onClick={onClose} style={{ marginLeft: 'auto' }}>
            <X size={16} />
          </button>
        </div>
        <div className="modal-tabs">
          <button className={tab === 'dag' ? 'active' : ''} onClick={() => setTab('dag')}>
            <FileCode2 size={13} /> DAG
          </button>
          <button className={tab === 'report' ? 'active' : ''} onClick={() => setTab('report')}>
            <FileText size={13} /> Report
          </button>
        </div>
        <div className="modal-body">
          {tab === 'dag' ? (
            <pre>{result.dag_source}</pre>
          ) : (
            <div className="md-report">
              <ReactMarkdown remarkPlugins={[remarkGfm]} components={MD_COMPONENTS}>
                {result.report}
              </ReactMarkdown>
            </div>
          )}
        </div>
        <div className="modal-foot">
          <button onClick={() => download(result.dag_filename, result.dag_source)}>
            <Download size={13} /> Download .py
          </button>
          <button onClick={() => download(result.report_filename, result.report)}>
            <Download size={13} /> Download report
          </button>
          <button className="primary" onClick={onClose}>
            <X size={13} /> Close
          </button>
        </div>
      </div>
    </div>
  )
}

// ── App root ─────────────────────────────────────────────────────

export default function App() {
  return (
    <div className="app">
      <header className="topbar">
        <div className="topbar-brand">
          <div className="brand-icon">
            <Workflow size={16} color="white" />
          </div>
          pdi <span className="brand-arrow">→</span> airflow
        </div>
        <span className="topbar-badge">Migration Tool</span>
        <div className="topbar-spacer" />
        <div className="topbar-meta">
          <div className="topbar-meta-item">
            <Clock size={11} />
            Asia/Bangkok
          </div>
          <div className="topbar-divider" />
          <div className="topbar-meta-item">DMS conventions</div>
        </div>
      </header>
      <div className="workspace">
        <ReactFlowProvider>
          <Editor />
        </ReactFlowProvider>
      </div>
    </div>
  )
}
