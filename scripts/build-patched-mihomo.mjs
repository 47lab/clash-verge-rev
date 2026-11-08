#!/usr/bin/env node
/**
 * Builds the stable mihomo core from pinned v1.19.33 source with the listener
 * reload patch. The alpha sidecar keeps the official source.
 */
import { execFileSync } from 'node:child_process'
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const MIHOMO_VERSION = 'v1.19.33'
const MIHOMO_COMMIT = '3f27ef39f8c96bbfbc8b0b9bb10f5c9ef60149d8'
const REPO_ROOT = path.resolve(fileURLToPath(new URL('..', import.meta.url)))
const PATCH_FILE = path.join(
  REPO_ROOT,
  'patches/mihomo-v1.19.33-listener-reload.patch',
)
const CORE_VERSION =
  process.env.MIHOMO_CORE_VERSION || `${MIHOMO_VERSION}-47lab.listenerfix.1`
const GO_TOOLCHAIN = process.env.MIHOMO_GOTOOLCHAIN || 'go1.22.12'

const TARGETS = {
  'aarch64-apple-darwin': { goos: 'darwin', goarch: 'arm64' },
  'x86_64-apple-darwin': { goos: 'darwin', goarch: 'amd64' },
}

const target = process.argv[2]
if (!target || !TARGETS[target]) {
  console.error(
    `Usage: node scripts/build-patched-mihomo.mjs <${Object.keys(TARGETS).join('|')}>`,
  )
  process.exit(1)
}
if (!existsSync(PATCH_FILE)) {
  console.error(`Patch file not found: ${PATCH_FILE}`)
  process.exit(1)
}

const { goos, goarch } = TARGETS[target]
const output = path.join(
  REPO_ROOT,
  'src-tauri',
  'sidecar',
  `verge-mihomo-${target}`,
)

const run = (command, args, options = {}) =>
  execFileSync(command, args, { stdio: 'inherit', ...options })
const capture = (command, args, options = {}) =>
  execFileSync(command, args, { encoding: 'utf8', ...options }).trim()

const work = mkdtempSync(path.join(tmpdir(), 'mihomo-patched-'))
try {
  run(
    'git',
    [
      '-c',
      'advice.detachedHead=false',
      'clone',
      '--quiet',
      '--depth',
      '1',
      '--branch',
      MIHOMO_VERSION,
      'https://github.com/MetaCubeX/mihomo.git',
      work,
    ],
    { env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } },
  )
  const commit = capture('git', ['-C', work, 'rev-parse', 'HEAD'])
  if (commit !== MIHOMO_COMMIT) {
    throw new Error(
      `mihomo ${MIHOMO_VERSION} resolved to ${commit}, expected ${MIHOMO_COMMIT}`,
    )
  }

  run('git', [
    '-C',
    work,
    'apply',
    '--unidiff-zero',
    '--whitespace=nowarn',
    PATCH_FILE,
  ])

  const built = path.join(work, 'mihomo-patched')
  const buildTime = new Date().toISOString()
  const ldflags = [
    `-X 'github.com/metacubex/mihomo/constant.Version=${CORE_VERSION}'`,
    `-X 'github.com/metacubex/mihomo/constant.BuildTime=${buildTime}'`,
    '-w',
    '-s',
    '-buildid=',
  ].join(' ')
  run(
    'go',
    [
      'build',
      '-tags',
      'with_gvisor',
      '-trimpath',
      '-ldflags',
      ldflags,
      '-o',
      built,
      '.',
    ],
    {
      cwd: work,
      env: {
        ...process.env,
        CGO_ENABLED: '0',
        GOOS: goos,
        GOARCH: goarch,
        GOTOOLCHAIN: GO_TOOLCHAIN,
      },
    },
  )

  mkdirSync(path.dirname(output), { recursive: true })
  copyFileSync(built, output)
  chmodSync(output, 0o755)

  if (goos === process.platform && goarch === process.arch) {
    const version = capture(output, ['-v'])
    if (!version.includes(CORE_VERSION)) {
      throw new Error(`Built core did not report ${CORE_VERSION}: ${version}`)
    }
    console.log(version.split('\n')[0])
  }
} finally {
  rmSync(work, { recursive: true, force: true })
}

console.log(
  `Patched stable core written to ${path.relative(REPO_ROOT, output)}`,
)
