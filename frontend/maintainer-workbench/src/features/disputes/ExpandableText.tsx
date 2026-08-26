import { useState } from 'react'

type ExpandableTextProps = {
  children?: string | null
  emptyLabel?: string
  limit?: number
  className?: string
}

export function ExpandableText({
  children,
  emptyLabel = 'Not provided',
  limit = 180,
  className = '',
}: ExpandableTextProps) {
  const [expanded, setExpanded] = useState(false)
  const value = children?.trim()
  if (!value) return <span className={className}>{emptyLabel}</span>

  const truncated = value.length > limit
  const visible = truncated && !expanded ? `${value.slice(0, limit).trimEnd()}…` : value
  return (
    <span className={className}>
      <span className="whitespace-pre-wrap">{visible}</span>
      {truncated ? (
        <button
          type="button"
          className="ml-1.5 whitespace-nowrap text-xs font-medium text-blue-700 hover:underline"
          aria-expanded={expanded}
          onClick={() => setExpanded((current) => !current)}
        >
          {expanded ? '收起全文' : '展开全文'}
        </button>
      ) : null}
    </span>
  )
}
