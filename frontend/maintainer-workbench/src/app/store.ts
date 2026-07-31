import { create } from 'zustand'

type WorkbenchUiState = {
  lastUpdatedAt: string
  sidebarCollapsed: boolean
  mobileNavOpen: boolean
  markUpdated: () => void
  toggleSidebar: () => void
  openMobileNav: () => void
  closeMobileNav: () => void
}

export const useWorkbenchUiStore = create<WorkbenchUiState>((set) => ({
  lastUpdatedAt: new Date().toISOString(),
  sidebarCollapsed: false,
  mobileNavOpen: false,
  markUpdated: () => set({ lastUpdatedAt: new Date().toISOString() }),
  toggleSidebar: () => set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
  openMobileNav: () => set({ mobileNavOpen: true }),
  closeMobileNav: () => set({ mobileNavOpen: false }),
}))
