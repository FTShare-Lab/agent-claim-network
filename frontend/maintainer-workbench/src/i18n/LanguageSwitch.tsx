// 登录页与主界面共享同一偏好，跨页面与同源标签同步。
import { Languages } from 'lucide-react'
import { setLanguage, useLanguage } from './index'

export function LanguageSwitch() {
  const language = useLanguage()
  return <label className="inline-flex shrink-0 items-center gap-1 rounded-lg border border-slate-200 bg-white px-2 text-xs text-slate-700">
    <Languages size={15} aria-hidden="true" />
    <span className="sr-only">{language === 'zh' ? '界面语言' : 'Interface language'}</span>
    <select value={language} onChange={(event) => setLanguage(event.target.value === 'en' ? 'en' : 'zh')} className="h-9 min-w-16 bg-transparent outline-offset-2">
      <option value="zh">中文</option><option value="en">English</option>
    </select>
  </label>
}
