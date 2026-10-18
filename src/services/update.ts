import {
  check,
  type CheckOptions,
  type Update,
} from '@tauri-apps/plugin-updater'
import { compareVersions as compareSemver } from 'compare-versions'

import { version as appVersion } from '@root/package.json'

const SEMVER_FULL_REGEX =
  /^\d+(?:\.\d+){1,2}(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/
const SEMVER_SEARCH_REGEX =
  /v?\d+(?:\.\d+){1,2}(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?/i

const normalizeVersion = (input: string | null | undefined): string | null => {
  if (typeof input !== 'string') return null
  const trimmed = input.trim()
  if (!trimmed) return null
  return trimmed.replace(/^v/i, '')
}

const ensureSemver = (input: string | null | undefined): string | null => {
  const normalized = normalizeVersion(input)
  if (!normalized) return null
  return SEMVER_FULL_REGEX.test(normalized) ? normalized : null
}

const extractSemver = (input: string | null | undefined): string | null => {
  if (typeof input !== 'string') return null
  const match = input.match(SEMVER_SEARCH_REGEX)
  if (!match) return null
  return normalizeVersion(match[0])
}

const compareVersions = (a: string | null, b: string | null): number | null => {
  if (!a || !b) return null
  try {
    return compareSemver(a, b)
  } catch {
    return null
  }
}

const resolveRemoteVersion = (update: Update): string | null => {
  const primary = ensureSemver(update.version)
  if (primary) return primary

  const fallbackPrimary = extractSemver(update.version)
  if (fallbackPrimary) return fallbackPrimary

  const raw = update.rawJson ?? {}
  const rawVersion = ensureSemver(
    typeof raw.version === 'string' ? raw.version : null,
  )
  if (rawVersion) return rawVersion

  const tagVersion = extractSemver(
    typeof raw.tag_name === 'string' ? raw.tag_name : null,
  )
  if (tagVersion) return tagVersion

  const nameVersion = extractSemver(
    typeof raw.name === 'string' ? raw.name : null,
  )
  if (nameVersion) return nameVersion

  return null
}

const parseForkVersion = (input: string | null | undefined) => {
  const version = ensureSemver(input)
  const match = version?.match(
    /^(\d+\.\d+\.\d+)-47lab\.([1-9]\d*)\.([1-9]\d*)$/,
  )
  if (!match) return null

  return {
    base: match[1],
    run: Number(match[2]),
    attempt: Number(match[3]),
  }
}

export const isForkUpdateAvailable = (
  current: string | null | undefined,
  remote: string | null | undefined,
): boolean => {
  const candidate = parseForkVersion(remote)
  if (!candidate) return false

  const installed = parseForkVersion(current)
  if (installed) {
    const baseComparison = compareVersions(candidate.base, installed.base)
    if (baseComparison === null) return false
    if (baseComparison !== 0) return baseComparison > 0
    return (
      candidate.run > installed.run ||
      (candidate.run === installed.run && candidate.attempt > installed.attempt)
    )
  }

  const currentBase = ensureSemver(current)?.split(/[-+]/, 1)[0] ?? null
  const baseComparison = compareVersions(candidate.base, currentBase)
  return baseComparison !== null && baseComparison >= 0
}

const localVersionNormalized = normalizeVersion(appVersion)

export const checkUpdateSafe = async (
  options?: CheckOptions,
): Promise<Update | null> => {
  const result = await check(options ?? {})
  if (!result) return null

  const remoteVersion = resolveRemoteVersion(result)
  if (!isForkUpdateAvailable(localVersionNormalized, remoteVersion)) {
    try {
      await result.close()
    } catch (err) {
      console.warn('[updater] failed to close stale update resource', err)
    }
    return null
  }

  return result
}

export type { CheckOptions }
