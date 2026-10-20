import { useT } from '../i18n'

export function LoadingWorkbench() {
  const t = useT()
  return <p role="status" className="p-6 text-sm text-slate-500">{t('Loading workbench…')}</p>
}
