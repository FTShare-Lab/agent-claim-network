// 语言仅属于浏览器展示偏好；不参与 API 参数、查询键或业务对象转换。
import { useSyncExternalStore } from 'react'
import { zh } from './zh'

export type Language = 'zh' | 'en'
export const LANGUAGE_KEY = 'acn.workbench.language.v1'
const listeners = new Set<() => void>()
function readLanguage(): Language {
  try { return localStorage.getItem(LANGUAGE_KEY) === 'en' ? 'en' : 'zh' } catch { return 'zh' }
}
let language = readLanguage()
function notify() {
  document.documentElement.lang = language === 'zh' ? 'zh-CN' : 'en'
  for (const listener of listeners) listener()
}
export function setLanguage(next: Language) {
  language = next
  try { localStorage.setItem(LANGUAGE_KEY, next) } catch { /* 禁用存储时仍允许当前页面切换。 */ }
  notify()
}
if (typeof window !== 'undefined') {
  document.documentElement.lang = language === 'zh' ? 'zh-CN' : 'en'
  window.addEventListener('storage', (event) => {
    if (event.key === LANGUAGE_KEY || event.key === null) { language = readLanguage(); notify() }
  })
}
const subscribe = (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener) } }
export function useLanguage() { return useSyncExternalStore(subscribe, () => language, () => 'zh' as Language) }
export function translate(locale: Language, key: string, values?: Record<string, string | number>): string {
  const text = locale === 'zh' ? zh[key] ?? key : key
  return values ? text.replace(/\{(\w+)\}/g, (match, name: string) => String(values[name] ?? match)) : text
}
const translators = {
  zh: (key: string, values?: Record<string, string | number>) => translate('zh', key, values),
  en: (key: string, values?: Record<string, string | number>) => translate('en', key, values),
}
export function useT() { return translators[useLanguage()] }
