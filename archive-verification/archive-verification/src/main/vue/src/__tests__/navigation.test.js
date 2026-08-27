import {describe, it, expect} from 'vitest'
import {links, groups} from '../navigation'
import router from '../router'

const navPaths = [...links.map((l) => l.to), ...groups.flatMap((g) => g.items.map((i) => i.to))]
const routePaths = router.getRoutes().map((r) => r.path)

describe('navigation', () => {
  it('only links to paths the router actually serves', () => {
    expect(navPaths.filter((p) => !routePaths.includes(p))).toEqual([])
  })

  it('reaches every route except /error, which is only reached by the error handler', () => {
    expect(routePaths.filter((p) => !navPaths.includes(p))).toEqual(['/error'])
  })

  it('has no duplicate destinations', () => {
    expect(navPaths).toHaveLength(new Set(navPaths).size)
  })

  it('gives every entry a label and an icon', () => {
    for (const l of links) {
      expect(l.label, JSON.stringify(l)).toBeTruthy()
      expect(l.icon, JSON.stringify(l)).toMatch(/^mdi-/)
    }
    for (const g of groups) {
      expect(g.label, JSON.stringify(g)).toBeTruthy()
      expect(g.icon, JSON.stringify(g)).toMatch(/^mdi-/)
      expect(g.items.length, g.label).toBeGreaterThan(0)
      for (const i of g.items) expect(i.label, JSON.stringify(i)).toBeTruthy()
    }
  })
})
