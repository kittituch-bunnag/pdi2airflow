import { Handle, Position } from 'reactflow'
import {
  Play, Square, Circle, Database, Zap, Terminal,
  Code2, Clock, GitBranch, AlertCircle,
} from 'lucide-react'
import TechIcon from './TechIcon.jsx'

const KIND_CONFIG = {
  start:          { Icon: Play,        color: '#3fb950' },
  end:            { Icon: Square,      color: '#f85149' },
  dummy:          { Icon: Circle,      color: '#8b949e' },
  transformation: { Icon: Zap,         color: '#e3b341' },
  sql:            { Icon: Database,    color: '#2f81f7' },
  shell:          { Icon: Terminal,    color: '#a371f7' },
  eval:           { Icon: Code2,       color: '#f0883e' },
  waitfor:        { Icon: Clock,       color: '#8b949e' },
  subjob:         { Icon: GitBranch,   color: '#ec6cb9' },
  unknown:        { Icon: AlertCircle, color: '#f85149' },
}

const KIND_LABELS = {
  start: 'Start', end: 'End', dummy: 'Dummy',
  transformation: 'Transform', sql: 'SQL', shell: 'Shell',
  eval: 'Eval', waitfor: 'Wait For', subjob: 'Sub-job', unknown: 'Unknown',
}

export default function PdiNode({ data }) {
  const { Icon, color } = KIND_CONFIG[data.kind] || KIND_CONFIG.unknown
  const kindLabel = KIND_LABELS[data.kind] || data.kind

  const handleStyle = { background: color, border: '2px solid #0d1117', width: 10, height: 10 }

  return (
    <>
      <Handle type="target" position={Position.Left} style={handleStyle} />
      <div className={`node-card kind-${data.kind}`}>
        <div className="node-header">
          <div className="node-icon-wrap" style={{ color, background: `${color}1a` }}>
            <Icon size={11} strokeWidth={2.5} />
          </div>
          <span className="node-kind-label">{kindLabel}</span>
        </div>
        <div className="node-main-label">{data.label}</div>
        {data.kind === 'transformation' && data.steps?.length > 0 && (
          <div className="node-footer">
            <span className="node-steps">{data.steps.length} step{data.steps.length !== 1 ? 's' : ''}</span>
            {data.unmappedCount > 0 && (
              <span className="node-unmapped"> · {data.unmappedCount} unmapped</span>
            )}
          </div>
        )}
        {data.kind === 'sql' && data.config?.connection && (
          <div className="node-footer">
            <TechIcon type={data.config.db_type} size={11} />
            <span className="node-conn">{data.config.connection}</span>
          </div>
        )}
      </div>
      <Handle type="source" position={Position.Right} style={handleStyle} />
    </>
  )
}
