/**
 * Job output wake — a blocking `job_output(wait: true)` read returns as soon as
 * the awaited background job writes new output.
 *
 * Port of `@argszero/cordis-plugin-steer-preempt` v0.2.0 to dsh 0.2.2-rc.3.
 *
 * ## What changed on the platform, and why this is not a straight copy
 *
 * The upstream plugin closed two gaps around the same call site. On 0.2.2-rc.3
 * one of them is closed in core, and the other is now closable without the
 * compromise the upstream README documents:
 *
 *  - **User steer (upstream v0.1): now core behavior.** `job_output` itself
 *    subscribes to `agent/inbox/inserted`, releases its own wait when a
 *    `source.kind === 'user'` message is pending, and reports the release as
 *    `user steered` beside the job's unchanged state
 *    (`@deepseek-ai/dsh-tool-jobs/src/index.ts`, the `args.wait === true` branch).
 *    Re-implementing it here would only race core's release and replace core's
 *    structured result with a worse one, so this port deliberately does NOT
 *    re-implement the steer trigger.
 *
 *  - **New job output (upstream v0.2): still a gap, but a cheap one now.** The
 *    wait's only wake sources are still settlement, its own timeout, and that
 *    input release. What changed is the observation cost:
 *      * `jobs.events.subscribe` now delivers a `{ type: 'output' }` event on
 *        every committed append, carrying the ring's new `total` — upstream had
 *        only visible-set changes and had to poll a *consuming* read to notice
 *        output at all;
 *      * `jobs.readAt(id, from)` reads retained output without moving the model
 *        cursor — upstream's README states no non-consuming read existed.
 *    So this port triggers on the event and needs no poll and no speculative
 *    read. It consumes exactly once, at fire time, and hands that read to the
 *    model, which is the one consumption the model was about to make anyway.
 *
 * ## Mechanism
 *
 * Registered as a `tools/execute` around-wrapper (the seam the in-tree
 * `guard/timeout-policy` uses). For a blocking `job_output` call only:
 *
 *  1. Read the job's current ring total with `jobs.get` — non-consuming, and it
 *     also fences the caller (an unknown or foreign job id is left to the tool's
 *     own error).
 *  2. Swap a derived `AbortController.signal` into `exec.signal` for the
 *     delegated lifetime. The registry re-fuses the caller's signal, so a real
 *     turn cancel keeps its semantics; the caller signal is never touched.
 *  3. Arm before dispatch, subscribe for `{ type: 'output' }` events of that
 *     job, and wait. `minBytes` of new output arms the wake; `graceMs` then
 *     keeps waiting so a job that finishes inside that window returns its real
 *     result through the normal path.
 *  4. Only a still-live wait is cut: abort the derived signal, which makes
 *     `jobs.wait` reject. The registry converts the tool's throw into an error
 *     result, which this wrapper replaces with the observed output and a
 *     `JOB_OUTPUT_RESUMED` structured error. The background job is never killed
 *     and the model is told explicitly that this was neither a timeout nor a
 *     completion.
 *
 * A job that settles before the wake fires keeps its own result untouched, and a
 * call this plugin cannot observe (no jobs service, unreadable id) delegates with
 * zero overhead.
 *
 * Verified against dsh 0.2.2-rc.3 (`@deepseek-ai/dsh-tools` 0.2.2-rc.3
 * `src/events.ts` + `src/pipeline.ts`, `@deepseek-ai/dsh-tool-jobs` 0.2.2-rc.3
 * `src/index.ts`, `@deepseek-ai/dsh-jobs` 0.2.2-rc.3 `src/types.ts` +
 * `src/index.ts`, `@deepseek-ai/dsh-jobs-local` 0.2.2-rc.3 `src/index.ts`).
 *
 * @module @yczsj/job-output-wake
 */

/** Cordis plugin name used by loader diagnostics. */
export const name = 'job-output-wake'

/** The tool registry service this plugin wraps (`tools/execute`). */
export const inject = ['tools']

/** The only tool this plugin wakes: a blocking background-job read. */
const BLOCKING_TOOL = 'job_output'

/** Structured error code of the replacement result, so policy can route on it. */
export const JOB_OUTPUT_RESUMED = 'JOB_OUTPUT_RESUMED'

const DEFAULT_GRACE_MS = 1000
const DEFAULT_MIN_BYTES = 1
const MAX_GRACE_MS = 30_000
const MAX_MIN_BYTES = 1_048_576

/**
 * Coerce one numeric knob, keeping a hostile or absent value inside its range
 * instead of throwing at activation.
 * @param value - the row's raw config value.
 * @param fallback - default when the value is not a finite number.
 * @param min - inclusive lower bound.
 * @param max - inclusive upper bound.
 * @returns the bounded integer.
 */
function pickNumber(value, fallback, min, max) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return fallback
  return Math.min(max, Math.max(min, Math.trunc(value)))
}

/**
 * Resolve the row's config into every field with its validated default. The
 * plugin declares no `Config` schema so it takes no dependency on a schema
 * library; values are coerced here instead.
 * @param config - the row's raw `config`, if any.
 * @returns the resolved configuration.
 */
function resolveConfig(config) {
  const raw = config !== null && typeof config === 'object' ? config : {}
  return {
    enabled: raw.enabled !== false,
    graceMs: pickNumber(raw.graceMs, DEFAULT_GRACE_MS, 0, MAX_GRACE_MS),
    minBytes: pickNumber(raw.minBytes, DEFAULT_MIN_BYTES, 1, MAX_MIN_BYTES),
  }
}

/**
 * The job id a `job_output` call names.
 * @param args - the call's parsed arguments.
 * @returns the id, or an empty string when the call names none.
 */
function jobIdOf(args) {
  if (args === null || typeof args !== 'object') return ''
  const id = args.job_id
  return typeof id === 'string' ? id : ''
}

/**
 * Whether this call asks the tool to block.
 * @param args - the call's parsed arguments.
 * @returns true only for an explicit `wait: true`.
 */
function isBlockingWait(args) {
  return args !== null && typeof args === 'object' && args.wait === true
}

/** True for the three terminal job statuses. */
function isTerminal(status) {
  return status === 'completed' || status === 'killed' || status === 'failed'
}

/** Wait one bounded interval; the grace window is the only timer this plugin needs. */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Render a consumed read the way `job_output` renders one: stdout and unlabeled
 * chunks in order, then every stderr chunk in one `[stderr]` section, then the
 * dropped-output notice when retention or a producer gap lost bytes.
 *
 * Port of `renderModelDelta` in `@deepseek-ai/dsh-tool-jobs/src/render.ts`. Kept
 * byte-compatible so a wake reads like an ordinary read of the same output; if
 * that renderer changes, this function is the one place to follow it.
 *
 * @param chunks - chunks since the model cursor, in offset order.
 * @param lossy - whether bytes before `chunks` were evicted unread.
 * @param spillPaths - complete-stream files the job currently advertises.
 * @returns the delta text, possibly empty.
 */
function renderModelDelta(chunks, lossy, spillPaths) {
  const visible = chunks.filter(chunk => chunk.channel !== 'log')
  const out = visible.filter(chunk => chunk.channel !== 'stderr').map(chunk => chunk.text).join('')
  const err = visible.filter(chunk => chunk.channel === 'stderr').map(chunk => chunk.text).join('')
  const separator = out.length > 0 && !out.endsWith('\n') ? '\n' : ''
  const body = out + (err.length > 0 ? `${separator}[stderr]\n${err}` : '')
  if (!lossy && !visible.some(chunk => chunk.gapBefore === true)) return body
  const notice = `[some output was dropped from memory; full output: ${spillPaths.length > 0 ? spillPaths.join(', ') : '(unavailable)'}]`
  return `${body}${body.length > 0 && !body.endsWith('\n') ? '\n' : ''}${notice}`
}

/**
 * The consuming read as the model sees it: the delta, then the producer's result
 * once it settled. Mirrors `readBody` in `@deepseek-ai/dsh-tool-jobs`.
 * @param read - the registry's consuming read.
 * @returns the model-facing body text.
 */
function readBodyText(read) {
  const delta = renderModelDelta(read.chunks, read.lossy, read.job.output.spillPaths ?? [])
  return read.result === undefined
    ? delta
    : `${delta}${delta.length > 0 && !delta.endsWith('\n') ? '\n' : ''}${read.result}`
}

/**
 * The status line `job_output` renders: live progress while running, the
 * terminal reason once settled. Mirrors `statusLine` in
 * `@deepseek-ai/dsh-tool-jobs/src/render.ts`.
 * @param job - the job projection.
 * @returns a bracketed status line.
 */
function statusLine(job) {
  const detail = job.progress ?? job.detail
  return detail !== undefined ? `[status: ${job.status}, ${detail}]` : `[status: ${job.status}]`
}

/**
 * The structured result substituted when this plugin's wake wins.
 *
 * Shaped as a failure on purpose, like the in-tree `TOOL_TIMEOUT` replacement: a
 * wake is not the tool's own outcome, and an early return must stay
 * distinguishable from a completed wait, so the model cannot read "the wait
 * returned" as "the job is done". The observed output is carried in `content`
 * because the reading that noticed it is the model's only copy of it.
 *
 * @param job - the job projection after the consuming read.
 * @param body - the consumed output text.
 * @param bytes - new bytes since the wait began.
 * @returns the `isError` result with a `JOB_OUTPUT_RESUMED` error.
 */
function resumedResult(job, body, bytes) {
  const message = isTerminal(job.status)
    ? `the blocking wait for job ${job.id} ended early because the job produced ${bytes} bytes of new output, and the job has now finished; the output above is complete.`
    : `the blocking wait for job ${job.id} ended early because the job produced ${bytes} bytes of new output. This is not a timeout and not a completion - the job keeps running, and its completion notice still arrives on its own. To keep waiting, call job_output again with wait: true and a timeout_ms; to keep working, read with wait: false.`
  const text = `${body.length > 0 && !body.endsWith('\n') ? `${body}\n` : body}${statusLine(job)}\n${message}`
  return {
    content: [{ type: 'text', text }],
    isError: true,
    error: { message, info: { name: 'JobOutputResumedError', code: JOB_OUTPUT_RESUMED } },
  }
}

/**
 * The around-dispatch logic, exported separately from {@link apply} so it is
 * testable without a live Cordis context.
 *
 * @param ctx - the mounting context; resolved for `jobs` on every call so a
 *   composition without background jobs degrades to a plain delegate.
 * @param config - resolved plugin configuration.
 * @param exec - the around-dispatch view of the pending call.
 * @param next - the delegated dispatch.
 * @returns the tool's own result, or a `JOB_OUTPUT_RESUMED` replacement.
 */
export async function runWrapper(ctx, config, exec, next) {
  // Only a blocking job read holds the step for up to the wait cap.
  if (exec.name !== BLOCKING_TOOL) return next()
  if (!isBlockingWait(exec.arguments)) return next()
  const jobId = jobIdOf(exec.arguments)
  if (jobId.length === 0) return next()
  const jobs = ctx.get('jobs')
  if (jobs === undefined) return next()
  const caller = exec.agent?.id

  // Baseline: bytes already produced. Non-consuming, and it doubles as the
  // caller fence - an unknown or foreign id is the tool's error to report, not
  // this wrapper's. A job that already settled needs no wake: its wait returns
  // the terminal projection on its own.
  let baseline
  try {
    const view = jobs.get(jobId, caller)
    if (isTerminal(view.status)) return next()
    baseline = view.output.total
  } catch {
    return next()
  }

  const controller = new AbortController()
  const upstream = exec.signal
  let unsubscribe = () => {}
  try {
    // Arm before dispatch so an append can never land between arming and the
    // wait registering its own listener.
    let armedAt = 0
    let releaseArmed
    const armedGate = new Promise((resolve) => { releaseArmed = resolve })
    unsubscribe = jobs.events.subscribe(
      // `{ owner }` is exactly the caller's visible set (its own jobs plus every
      // unowned job); an agent-less call falls back to every owner and still
      // matches by id only.
      caller === undefined ? { owners: 'all' } : { owner: caller },
      (event) => {
        if (armedAt !== 0) return
        if (event.type !== 'output' || event.id !== jobId) return
        if (event.total - baseline < config.minBytes) return
        armedAt = Date.now()
        releaseArmed(armedAt)
      },
    )

    // Swap the derived signal onto exec for the delegated lifetime; the registry
    // re-fuses the caller's signal, so a real cancel keeps its semantics.
    exec.signal = controller.signal
    const dispatched = next()
    const settledGate = dispatched.then(() => 'settled', () => 'settled')

    // Phase 1: first qualifying output, or the wait settling on its own.
    if (await Promise.race([armedGate.then(() => 'armed'), settledGate]) === 'settled') {
      return await dispatched
    }

    // Phase 2: settle grace. A job that finishes inside the window keeps its own
    // real result; only a still-live wait is cut.
    if (config.graceMs > 0) {
      if (await Promise.race([sleep(config.graceMs).then(() => 'grace'), settledGate]) === 'settled') {
        return await dispatched
      }
    }

    let live = false
    try { live = !isTerminal(jobs.get(jobId, caller).status) } catch { live = false }
    if (!live) return await dispatched

    controller.abort()
    const result = await dispatched
    // A successful result means the tool collected the settlement itself.
    if (!result.isError) return result

    // The consuming read the trigger earns: the output is handed to the model
    // rather than discarded, and the tool will not read again (it threw).
    let read
    try { read = jobs.read(jobId, caller) } catch { return result }
    return resumedResult(read.job, readBodyText(read), read.job.output.total - baseline)
  } finally {
    unsubscribe()
    exec.signal = upstream
  }
}

/**
 * Register the wake wrapper.
 * @param ctx - the mounting context.
 * @param config - the row's raw `config`, if any.
 */
export function apply(ctx, config) {
  const resolved = resolveConfig(config)
  if (!resolved.enabled) return
  ctx.on('tools/execute', (exec, next) => runWrapper(ctx, resolved, exec, next))
}
