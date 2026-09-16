// HTTP 页面没有 Web Locks；用 IndexedDB 的独占事务协调同源标签的登录与注销。
const LOCK_NAME = 'acn-maintainer-admin-session'

export function withSessionLock<T>(action: () => Promise<T>): Promise<T> {
  if (navigator.locks) return navigator.locks.request(LOCK_NAME, action)
  return withDatabaseLock(action)
}

function withDatabaseLock<T>(action: () => Promise<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    const opening = indexedDB.open(LOCK_NAME, 1)
    opening.onupgradeneeded = () => { opening.result.createObjectStore('lock') }
    opening.onerror = () => reject(new Error('Cannot coordinate sign-in across tabs.'))
    opening.onsuccess = () => {
      const database = opening.result
      const transaction = database.transaction('lock', 'readwrite')
      const store = transaction.objectStore('lock')
      let started = false
      let outcome: { ok: true; value: T } | { ok: false; error: unknown } | undefined
      transaction.oncomplete = () => {
        database.close()
        if (outcome?.ok) resolve(outcome.value)
        else reject(outcome?.error ?? new Error('Sign-in coordination interrupted.'))
      }
      transaction.onabort = () => {
        database.close()
        reject(new Error('Sign-in coordination interrupted.'))
      }
      // fetch 等待期间持续排入只读请求，让独占事务保持存活。数据库不保存凭据或会话。
      // 标签关闭后浏览器自动结束事务，其他标签可继续；没有需要回收的租约或超时锁。
      const keepAlive = () => {
        const request = store.get('session')
        request.onsuccess = () => {
          if (!started) {
            started = true
            void Promise.resolve().then(action).then(
              (value) => { outcome = { ok: true, value } },
              (error: unknown) => { outcome = { ok: false, error } },
            )
          }
          if (!outcome) keepAlive()
        }
      }
      keepAlive()
    }
  })
}
