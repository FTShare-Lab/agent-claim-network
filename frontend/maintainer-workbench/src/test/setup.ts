import '@testing-library/jest-dom/vitest'

// Node 的 undici Request 与 jsdom AbortSignal 属于不同 realm。React Router 在
// memory-router 测试中会把后者传给前者；仅在品牌检查不兼容时去掉
// signal，避免测试导航在构造 Request 前失败。
const NativeRequest = globalThis.Request
if (NativeRequest) {
  class CompatibleRequest extends NativeRequest {
    constructor(input: RequestInfo | URL, init?: RequestInit) {
      let compatibleInit = init
      if (init?.signal) {
        try {
          new NativeRequest('http://localhost/', { signal: init.signal })
        } catch {
          compatibleInit = { ...init, signal: undefined }
        }
      }
      super(input, compatibleInit)
    }
  }
  globalThis.Request = CompatibleRequest
}
