// 鼠标在画布空白处拖拽时平移；节点按钮保留点击，触屏保留原生滚动。
import { useRef, useState, type PointerEvent } from 'react'

export function useCanvasPan() {
  const drag = useRef<{ pointerId: number; x: number; y: number; left: number; top: number } | null>(null)
  const [isPanning, setIsPanning] = useState(false)

  function stopPan(event: PointerEvent<HTMLDivElement>) {
    if (drag.current?.pointerId !== event.pointerId) return
    drag.current = null
    setIsPanning(false)
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId)
  }

  return {
    isPanning,
    onPointerDown(event: PointerEvent<HTMLDivElement>) {
      if (event.pointerType !== 'mouse' || event.button !== 0) return
      if (event.target instanceof Element && event.target.closest('button, a, input, select, textarea')) return
      const element = event.currentTarget
      event.preventDefault()
      element.focus({ preventScroll: true })
      drag.current = { pointerId: event.pointerId, x: event.clientX, y: event.clientY, left: element.scrollLeft, top: element.scrollTop }
      element.setPointerCapture(event.pointerId)
      setIsPanning(true)
    },
    onPointerMove(event: PointerEvent<HTMLDivElement>) {
      const start = drag.current
      if (!start || start.pointerId !== event.pointerId) return
      event.currentTarget.scrollLeft = start.left - (event.clientX - start.x)
      event.currentTarget.scrollTop = start.top - (event.clientY - start.y)
    },
    onPointerUp: stopPan,
    onPointerCancel: stopPan,
    onLostPointerCapture: stopPan,
  }
}
