import { Database } from 'lucide-react'
import {
  siPostgresql,
  siMysql,
  siMariadb,
  siSqlite,
  siApacheairflow,
  siMongodb,
  siRedis,
} from 'simple-icons'

const SI_MAP = {
  postgresql:  siPostgresql,
  postgres:    siPostgresql,
  mysql:       siMysql,
  mariadb:     siMariadb,
  sqlite:      siSqlite,
  airflow:     siApacheairflow,
  apacheairflow: siApacheairflow,
  mongodb:     siMongodb,
  redis:       siRedis,
}

// Custom icons for brands not in simple-icons
function OracleIcon({ size }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none">
      <ellipse cx="12" cy="12" rx="10" ry="6.5" fill="#F80000" />
      <text x="12" y="15.5" textAnchor="middle" fill="white" fontSize="7" fontWeight="bold" fontFamily="Arial,sans-serif">OCI</text>
    </svg>
  )
}

function MssqlIcon({ size }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="3" fill="#CC2927" />
      <text x="12" y="16" textAnchor="middle" fill="white" fontSize="8.5" fontWeight="bold" fontFamily="Arial,sans-serif">SQL</text>
    </svg>
  )
}

function PdiIcon({ size }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none">
      <ellipse cx="12" cy="7.5" rx="4.5" ry="4.5" fill="#006BB6" />
      <rect x="10.75" y="11.5" width="2.5" height="8.5" rx="1.25" fill="#006BB6" />
    </svg>
  )
}

function SiBrand({ icon, size }) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={`#${icon.hex}`} aria-label={icon.title}>
      <path d={icon.path} />
    </svg>
  )
}

export default function TechIcon({ type, size = 16 }) {
  if (!type) return <Database size={size} color="#8b949e" />

  const normalized = type.toLowerCase().replace(/[\s_-]/g, '')

  if (['pdi', 'pentaho', 'kettle', 'spoon'].includes(normalized)) return <PdiIcon size={size} />
  if (['oracle', 'oracledatabase'].includes(normalized)) return <OracleIcon size={size} />
  if (['mssql', 'sqlserver', 'microsoftsqlserver', 'mssqlserver'].includes(normalized)) return <MssqlIcon size={size} />

  const icon = SI_MAP[normalized]
  if (icon) return <SiBrand icon={icon} size={size} />

  return <Database size={size} color="#8b949e" />
}
