import type { LucideIcon } from 'lucide-react'
import {
  Activity,
  BadgeAlert,
  FileClock,
  Files,
  LayoutDashboard,
  Network,
  Settings,
  ShieldCheck,
  KeyRound,
  Users,
} from 'lucide-react'

export type NavItem = {
  label: string
  description: string
  href: string
  icon: LucideIcon
}

export type NavSection = {
  title: string
  items: NavItem[]
}

export const navSections: NavSection[] = [
  {
    title: 'Observe',
    items: [
      {
        label: 'Overview',
        description: 'Network Overview',
        href: '/',
        icon: LayoutDashboard,
      },
      { label: 'Claims', description: 'Claim corpus', href: '/claims', icon: Files },
      { label: 'Agents', description: 'Agent inventory', href: '/agents', icon: Users },
    ],
  },
  {
    title: 'Govern',
    items: [
      { label: 'Disputes', description: 'Review and resolve', href: '/disputes', icon: BadgeAlert },
      { label: 'Policies', description: 'Policy control', href: '/policies', icon: ShieldCheck },
      { label: 'Team Auth', description: 'Agent access keys', href: '/team-auth', icon: KeyRound },
    ],
  },
  {
    title: 'Diagnose',
    items: [
      { label: 'Sweep', description: 'Claim aging checks', href: '/sweep', icon: FileClock },
      { label: 'Router Query', description: 'Retrieval inspection', href: '/router-query', icon: Network },
      { label: 'HTTP Audits', description: 'Request traces', href: '/http-audits', icon: Activity },
    ],
  },
  {
    title: 'System',
    items: [
      { label: 'Settings', description: 'Runtime and endpoints', href: '/settings', icon: Settings },
    ],
  },
]
