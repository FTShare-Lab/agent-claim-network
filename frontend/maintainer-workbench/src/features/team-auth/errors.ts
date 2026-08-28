export function teamAuthErrorMessage(error: unknown) {
  if (!error) return null
  const message = (error instanceof Error ? error.message : String(error)).trim()
  if (!message) return null
  const sentence = `${message.charAt(0).toUpperCase()}${message.slice(1)}`
  return /[.!?。！？]$/.test(sentence) ? sentence : `${sentence}.`
}
