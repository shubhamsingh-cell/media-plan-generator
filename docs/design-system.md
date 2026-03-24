# Nova AI Suite -- Design System

## Brand Colors
| Token | Value | Usage |
|-------|-------|-------|
| --port-gore | #202058 | Deep brand blue |
| --blue-violet / --accent | #5a54bd | Primary accent |
| --downy-teal / --teal | #6bb3cd | Secondary accent |

## Surfaces
| Token | Value | Usage |
|-------|-------|-------|
| --bg-root | #09090b | Page background |
| --bg-sidebar | #0c0c14 | Sidebar background |
| --bg-card | rgba(22,22,36,0.65) | Card backgrounds |
| --bg-input | rgba(255,255,255,0.04) | Input backgrounds |

## Text
| Token | Value | Usage |
|-------|-------|-------|
| --text-primary | #e4e4e7 | Headings, body text |
| --text-secondary | #a1a1aa | Subtitles, labels |
| --text-tertiary | #71717a | Muted text, hints |

## Status Colors
| Token | Value | Usage |
|-------|-------|-------|
| --green | #22c55e | Success, healthy |
| --amber | #f59e0b | Warning, needs attention |
| --red | #ef4444 | Error, critical |

## Spacing
- --radius-sm: 6px
- --radius-md: 10px
- --radius-lg: 14px
- --radius-xl: 20px

## Typography
- Font: Inter (300-800 weights)
- Base size: 14px
- Line height: 1.6

## Transitions
- --transition-fast: 150ms cubic-bezier(0.4, 0, 0.2, 1)
- --transition-base: 200ms
- --transition-slow: 350ms

## Component Patterns
- Cards: glassmorphism with backdrop-filter blur(16px)
- Buttons: rounded with accent gradient on hover
- Inputs: subtle background with border-active on focus
- Sidebar: collapsible (252px -> 64px), tooltip on collapsed hover

## Product Group Colors
- Plan: --accent-glow (purple tint)
- Intelligence: --teal-glow (teal tint)
- Compliance: --red-glow (red tint)

## Accessibility
- prefers-reduced-motion: respected globally
- Focus visible: 2px solid accent, 2px offset
- Skip navigation: hidden link, visible on focus
- ARIA landmarks on all major sections
