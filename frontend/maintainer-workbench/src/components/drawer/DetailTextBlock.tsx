import { cn } from '../../lib/utils'

type DetailTextBlockProps = {
  children: string
  className?: string
}

export function DetailTextBlock({ children, className }: DetailTextBlockProps) {
  return (
    <div
      className={cn(
        'whitespace-pre-wrap break-words rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 font-mono text-xs leading-5 text-slate-700',
        className,
      )}
    >
      {children}
    </div>
  )
}
