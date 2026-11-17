// Standalone behavioural tests for the wake wrapper, run with:
//   ELECTRON_RUN_AS_NODE=1 <node> test/run.mjs
// The wrapper is context-free enough to exercise with a fake jobs service and a
// fake dispatch, so this checks the trigger, grace, settle-wins, and delegate
// paths without a live harness.
import assert from 'node:assert/strict'
import { runWrapper, JOB_OUTPUT_RESUMED } from '../index.js'

const CONFIG = { enabled: true, graceMs: 0, minBytes: 1 }
const sleep = (ms) => new Promise(r => setTimeout(r, ms))

/** A job projection with no progress line. */
const view = (status = 'running', total = 0, extra = {}) => ({
  id: 'bash-1',
  kind: 'bash',
  label: 'npm run build',
  status,
  startedAt: 1,
  output: { total, earliest: 0 },
  ...extra,
})

/** A fake ctx exposing only what the wrapper resolves. */
function fakeCtx(jobs) {
  return { get: (name) => (name === 'jobs' ? jobs : undefined) }
}

/**
 * A fake job registry plus the fake `next()` that stands in for
 * `job_output`'s body: it waits until aborted or until `settleAfter` elapses.
 */
function harness({ baseline = 0, settleAfter, readChunks = [], statusAfterAbort = 'running', getThrows = false }) {
  const state = { listeners: [], unsubscribed: 0, nextCalls: 0, reads: 0, aborted: 0, total: baseline }
  const jobs = {
    get() {
      if (getThrows) throw new Error('unknown job bash-1')
      // Before the wait settles the job is running; after, the harness reports
      // the status the caller asked for.
      return view(state.settled === true ? statusAfterAbort : 'running', state.total)
    },
    read() {
      state.reads += 1
      const total = state.total
      return { chunks: readChunks, lossy: false, job: view(state.settled === true ? statusAfterAbort : 'running', total) }
    },
    events: {
      subscribe(filter, listener) {
        state.filter = filter
        state.listeners.push(listener)
        return () => { state.unsubscribed += 1; state.listeners = state.listeners.filter(l => l !== listener) }
      },
    },
  }
  const emitOutput = (bytes) => {
    state.total += bytes
    for (const listener of [...state.listeners]) listener({ type: 'output', id: 'bash-1', owner: 's1', total: state.total })
  }
  const exec = { name: 'job_output', arguments: { job_id: 'bash-1', wait: true }, agent: { id: 's1' }, signal: new AbortController().signal }
  const next = () => {
    state.nextCalls += 1
    return new Promise((resolve) => {
      let done = false
      const finish = (value) => { if (!done) { done = true; resolve(value) } }
      const settle = () => { state.settled = true; finish({ isError: false, content: [{ type: 'text', text: 'real result' }], value: {} }) }
      exec.signal.addEventListener('abort', () => {
        state.aborted += 1
        state.settled = state.settled === true
        // The registry reports an abort as a failed result, never a throw.
        finish({ isError: true, error: { message: 'wait aborted', info: { name: 'AbortError', code: 'ABORTED' } }, content: [] })
      }, { once: true })
      if (settleAfter !== undefined) setTimeout(settle, settleAfter)
    })
  }
  return { jobs, exec, next, state, emitOutput, ctx: fakeCtx(jobs) }
}

const tests = []
const test = (name, fn) => tests.push({ name, fn })

test('no new output and a settling job keeps its real result', async () => {
  const h = harness({ settleAfter: 60 })
  const result = await runWrapper(h.ctx, CONFIG, h.exec, h.next)
  assert.equal(result.isError, false)
  assert.equal(result.content[0].text, 'real result')
  assert.equal(h.state.aborted, 0)
  assert.equal(h.state.reads, 0)
  assert.equal(h.state.unsubscribed, 1, 'subscription must be released')
  assert.deepEqual(h.state.filter, { owner: 's1' })
})

test('new output wakes the wait with the consumed delta', async () => {
  const h = harness({ baseline: 0, settleAfter: 5000, readChunks: [{ at: 0, text: 'compiled 3 files\n' }] })
  const pending = runWrapper(h.ctx, CONFIG, h.exec, h.next)
  await sleep(10)
  h.emitOutput(18)
  const result = await pending
  assert.equal(result.isError, true)
  assert.equal(result.error.info.code, JOB_OUTPUT_RESUMED)
  assert.equal(result.error.info.name, 'JobOutputResumedError')
  const text = result.content[0].text
  assert.match(text, /compiled 3 files/)
  assert.match(text, /\[status: running\]/)
  assert.match(text, /not a timeout and not a completion/)
  assert.equal(h.state.aborted, 1)
  assert.equal(h.state.reads, 1)
})

test('output below minBytes does not arm the wake', async () => {
  const h = harness({ baseline: 0, settleAfter: 80, readChunks: [{ at: 0, text: 'x' }] })
  const pending = runWrapper(h.ctx, { ...CONFIG, minBytes: 10 }, h.exec, h.next)
  await sleep(10)
  h.emitOutput(3)
  const result = await pending
  assert.equal(result.isError, false, 'the settle must win when the wake never arms')
  assert.equal(h.state.aborted, 0)
})

test('a settle inside the grace window keeps the real result', async () => {
  const h = harness({ baseline: 0, settleAfter: 40, readChunks: [{ at: 0, text: 'late' }] })
  const pending = runWrapper(h.ctx, { ...CONFIG, graceMs: 200 }, h.exec, h.next)
  await sleep(10)
  h.emitOutput(5)
  const result = await pending
  assert.equal(result.isError, false)
  assert.equal(h.state.aborted, 0)
  assert.equal(h.state.reads, 0)
})

test('output that arrived before the wait began is not new', async () => {
  const h = harness({ baseline: 40, settleAfter: 80 })
  const pending = runWrapper(h.ctx, CONFIG, h.exec, h.next)
  await sleep(10)
  // An append that only reaches the pre-existing baseline must not arm.
  for (const listener of [...h.state.listeners]) listener({ type: 'output', id: 'bash-1', owner: 's1', total: 40 })
  const result = await pending
  assert.equal(result.isError, false)
  assert.equal(h.state.aborted, 0)
})

test('a non-blocking call, another tool, and a foreign job all delegate', async () => {
  for (const mutate of [
    (h) => { h.exec.arguments = { job_id: 'bash-1' } },
    (h) => { h.exec.name = 'job_list' },
    (h) => { h.exec.arguments = { job_id: 'bash-1', wait: true }; h.jobs.get = () => { throw new Error('foreign') } },
  ]) {
    const h = harness({ settleAfter: 20 })
    mutate(h)
    const result = await runWrapper(h.ctx, CONFIG, h.exec, h.next)
    assert.equal(result.isError, false)
    assert.equal(h.state.nextCalls, 1)
    assert.equal(h.state.unsubscribed, 0, 'a delegate must not subscribe')
  }
})

test('no jobs service delegates', async () => {
  const h = harness({ settleAfter: 20 })
  const result = await runWrapper({ get: () => undefined }, CONFIG, h.exec, h.next)
  assert.equal(result.isError, false)
  assert.equal(h.state.nextCalls, 1)
})

test('a job already terminal at call time delegates', async () => {
  const h = harness({ settleAfter: 20 })
  h.jobs.get = () => view('completed', 10)
  const result = await runWrapper(h.ctx, CONFIG, h.exec, h.next)
  assert.equal(result.isError, false)
  assert.equal(h.state.nextCalls, 1)
})

test('stderr and log chunks render like job_output', async () => {
  const h = harness({
    settleAfter: 5000,
    readChunks: [
      { at: 0, text: 'out', channel: 'stdout' },
      { at: 3, text: 'noise', channel: 'log' },
      { at: 8, text: 'bad\n', channel: 'stderr' },
    ],
  })
  const pending = runWrapper(h.ctx, CONFIG, h.exec, h.next)
  await sleep(10)
  h.emitOutput(13)
  const text = (await pending).content[0].text
  assert.match(text, /out\n\[stderr\]\nbad\n/)
  assert.doesNotMatch(text, /noise/)
})

let failed = 0
for (const { name, fn } of tests) {
  try {
    await fn()
    console.log(`ok   - ${name}`)
  } catch (error) {
    failed += 1
    console.log(`FAIL - ${name}`)
    console.log(`       ${error.message}`)
  }
}
console.log(failed === 0 ? `\n${tests.length}/${tests.length} passed` : `\n${failed} failed`)
process.exit(failed === 0 ? 0 : 1)
