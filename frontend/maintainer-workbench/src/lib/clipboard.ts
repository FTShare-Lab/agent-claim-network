export async function copyTextToClipboard(text: string) {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text)
      return true
    } catch {
      // HTTP 内网 IP 等非安全上下文会走到这里，继续尝试兼容路径。
    }
  }

  return copyTextWithSelectionFallback(text)
}

function copyTextWithSelectionFallback(text: string) {
  if (!document.body || typeof document.execCommand !== 'function') {
    return false
  }

  const textArea = document.createElement('textarea')
  textArea.value = text
  textArea.setAttribute('readonly', '')
  textArea.style.position = 'fixed'
  textArea.style.left = '-9999px'
  textArea.style.top = '0'
  textArea.style.opacity = '0'

  document.body.appendChild(textArea)
  textArea.focus()
  textArea.select()
  textArea.setSelectionRange(0, text.length)

  try {
    return document.execCommand('copy')
  } catch {
    return false
  } finally {
    textArea.remove()
  }
}
