import { translate, type Language } from '../i18n'
const dateFormatter = new Intl.DateTimeFormat('en-CA', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
})

export function formatDateTime(value: string | null | undefined) {
  if (!value) return 'N/A'
  return dateFormatter.format(new Date(value)).replace(',', '')
}

export function formatRelativeMinutes(value: string | null | undefined, language: Language = 'en') {
  if (!value) return translate(language, 'Unavailable')
  const minutes = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 60000))
  if (minutes < 1) return translate(language, 'just now')
  if (minutes < 60) return translate(language, '{value}m ago', { value: minutes })
  const hours = Math.round(minutes / 60)
  if (hours < 24) return translate(language, '{value}h ago', { value: hours })
  const days = Math.round(hours / 24)
  if (days < 30) return translate(language, '{value}d ago', { value: days })
  const months = Math.round(days / 30)
  if (months < 12) return translate(language, '{value}mo ago', { value: months })
  return translate(language, '{value}y ago', { value: Math.round(months / 12) })
}

export function clampPercent(value: number) {
  return Math.max(0, Math.min(100, Math.round(value)))
}

export function toPercent(numerator: number, denominator: number) {
  if (denominator <= 0) return 0
  return clampPercent((numerator / denominator) * 100)
}

export function statusTone(input: string) {
  const value = input.toLowerCase()
  if (['open', 'high', 'lagging', 'error', 'unhealthy'].includes(value)) return 'danger'
  if (['medium', 'stale', 'idle', 'warning'].includes(value)) return 'warning'
  if (['resolved', 'active', 'healthy', 'success', 'live'].includes(value)) return 'success'
  if (['deprecated', 'muted', 'unknown', 'n/a'].includes(value)) return 'neutral'
  return 'info'
}

export function truncateMiddle(value: string, max = 22) {
  if (value.length <= max) return value
  const head = Math.ceil((max - 3) / 2)
  const tail = Math.floor((max - 3) / 2)
  return `${value.slice(0, head)}...${value.slice(-tail)}`
}
