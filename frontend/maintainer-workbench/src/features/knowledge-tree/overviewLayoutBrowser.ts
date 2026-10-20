// 布局在独立 Worker 中运行，切换页面或请求超时后及时释放。
import ELK from 'elkjs/lib/elk-api'
import workerUrl from 'elkjs/lib/elk-worker.min.js?url'
import { layoutOverview, type OverviewTopology } from './overviewLayout'

export async function loadOverviewLayout(topology: OverviewTopology, aspectRatio: number, signal: AbortSignal) {
  signal.throwIfAborted()
  const engine = new ELK({ workerFactory: () => new Worker(workerUrl) })
  let timeout: ReturnType<typeof setTimeout> | undefined
  let abort: () => void = () => {}
  try {
    return await Promise.race([
      layoutOverview(topology, aspectRatio, engine, signal),
      new Promise<never>((_, reject) => {
        abort = () => reject(signal.reason)
        signal.addEventListener('abort', abort, { once: true })
        timeout = setTimeout(() => reject(new Error('Overview layout timed out')), 20000)
      }),
    ])
  } finally {
    clearTimeout(timeout)
    signal.removeEventListener('abort', abort)
    engine.terminateWorker()
  }
}
