import { describe, expect, test, vi } from 'vitest'

vi.mock('@tauri-apps/plugin-updater', () => ({ check: vi.fn() }))

import { isForkUpdateAvailable } from './update'

describe('fork updater version ordering', () => {
  test('allows the first personal revision from a matching base version', () => {
    expect(isForkUpdateAvailable('2.5.8', '2.5.8-47lab.10.1')).toBe(true)
    expect(
      isForkUpdateAvailable('2.5.8+47lab.legacy.r4', '2.5.8-47lab.10.1'),
    ).toBe(true)
  })

  test('orders personal run and retry identifiers numerically', () => {
    expect(
      isForkUpdateAvailable('2.5.8-47lab.10.1', '2.5.8-47lab.10.2'),
    ).toBe(true)
    expect(
      isForkUpdateAvailable('2.5.8-47lab.9.5', '2.5.8-47lab.10.1'),
    ).toBe(true)
    expect(
      isForkUpdateAvailable('2.5.8-47lab.10.1', '2.5.8-47lab.9.9'),
    ).toBe(false)
    expect(
      isForkUpdateAvailable('2.5.8-47lab.10.1', '2.5.8-47lab.10.1'),
    ).toBe(false)
  })

  test('accepts newer upstream bases only through fork revisions', () => {
    expect(
      isForkUpdateAvailable('2.5.8-47lab.10.1', '2.5.9-47lab.1.1'),
    ).toBe(true)
    expect(isForkUpdateAvailable('2.5.8-47lab.10.1', '2.5.9')).toBe(false)
    expect(
      isForkUpdateAvailable('2.5.8-47lab.10.1', '2.5.7-47lab.20.1'),
    ).toBe(false)
  })

  test('rejects malformed personal revisions', () => {
    expect(isForkUpdateAvailable('2.5.8', '2.5.8-47lab.0.1')).toBe(false)
    expect(isForkUpdateAvailable('2.5.8', '2.5.8-47lab.1')).toBe(false)
  })
})
