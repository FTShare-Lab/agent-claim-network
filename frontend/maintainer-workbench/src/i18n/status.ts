// 仅用于领域状态的展示；筛选、API 请求和缓存始终使用原始枚举值。
import { useLanguage, type Language } from './index'

const labels: Record<string, string> = {
  active: '活跃', stale: '陈旧', deprecated: '废弃', deprecate: '废弃',
  open: '待处理', resolved: '已处理',
  live: '最近活跃', lagging: '最近未同步', idle: '长期未同步',
}
export function statusLabel(language: Language, value: string) {
  return language === 'zh' ? labels[value.toLowerCase()] ?? value : value
}
export function useStatusLabel() {
  const language = useLanguage()
  return (value: string) => statusLabel(language, value)
}
