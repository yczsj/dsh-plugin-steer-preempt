# @yczsj/job-output-wake

A blocking `job_output(wait: true)` read returns **as soon as the awaited
background job writes new output**, instead of blocking for up to the wait cap.
The job keeps running; its completion notice still arrives.

This is a port of [`@argszero/cordis-plugin-steer-preempt`](https://github.com/argszero/cordis-plugin-steer-preempt)
v0.2.0 to **dsh 0.2.2-rc.3**. It is not a straight copy: one of that plugin's two
triggers is now core behavior, and the other can now be built without the
compromise the original README documents.

## Install

On the target machine, open **Plugins** in the sidebar and use **Add plugin**
with the repository address:

```
https://github.com/yczsj/dsh-plugin-steer-preempt
```

Add plugin also accepts a local checkout path and a `.tgz` tarball. The plugin
carries no dependencies and no build step, so nothing else is needed; the row
activates immediately and applies to every session in that profile.

It requires **dsh 0.2.x**: it uses the `output` job event, `jobs.get`, and the
discriminated `ToolExecutionResult` union. Behaviour was verified on
`0.2.2-rc.3`; an older 0.1.x install will not load it.

## What changed since 0.1.5, and what this port does about it

| Upstream trigger | Status on 0.2.2-rc.3 | This port |
|---|---|---|
| **User steer** (v0.1): a queued user message preempts the wait | **Core behavior.** `job_output` subscribes to `agent/inbox/inserted`, releases its own wait on a `source.kind === 'user'` message, and reports it as `user steered` beside the job's unchanged state | **Not re-implemented.** Doing so would race core's release and replace core's structured result with a worse one |
| **New job output** (v0.2): output appearing mid-wait wakes the read | Still a gap — the wait wakes only on settlement, its own timeout, or that input release | **Implemented, cheaply** (see below) |

Two platform additions make the output trigger much simpler than upstream's:

- `jobs.events.subscribe` delivers a `{ type: 'output' }` event on every committed
  append, carrying the ring's new `total`. Upstream had only visible-set changes
  and had to poll a **consuming** read to notice output at all.
- `jobs.readAt(id, from)` reads retained output **without moving the model
  cursor**. Upstream's README states that no non-consuming read existed, which is
  why it shipped `deliver: wrap|cut|off` modes around a read that had to be
  spent to observe.

So this port triggers on the event, never polls, and consumes exactly once — at
fire time — handing that read to the model. The output the model was waiting for
is delivered, not discarded.

## How it works

Registered as a `tools/execute` around-wrapper, the same seam the in-tree
`guard/timeout-policy` uses. For a blocking `job_output` call only:

1. Read the job's current ring total with `jobs.get` — non-consuming, and it
   doubles as the caller fence.
2. Swap a derived `AbortController.signal` into `exec.signal` for the delegated
   lifetime. The registry re-fuses the caller's signal, so a real turn cancel
   keeps its semantics; the caller's signal is never touched.
3. Arm before dispatch, subscribe for that job's `output` events, and wait.
   `minBytes` of new output arms the wake; `graceMs` then keeps waiting so a job
   that finishes inside that window returns its real result normally.
4. Only a still-live wait is cut. Aborting the derived signal makes `jobs.wait`
   reject; the registry converts the tool's throw into an error result, which
   this wrapper replaces with the consumed output plus a `JOB_OUTPUT_RESUMED`
   structured error.

The replacement is shaped as a failure on purpose, like the in-tree
`TOOL_TIMEOUT`: a wake is not the tool's own outcome, and an early return must
stay distinguishable from a completed wait. Its text says so explicitly:
*this is not a timeout and not a completion; the job keeps running.*

## Configuration

All fields are optional; the patch defaults are shown.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `true` | `false` mounts the row but wraps nothing |
| `graceMs` | `1000` | Settle window: after the wake arms, how long to keep waiting so a nearly-finished job keeps its real result. `0` cuts at once |
| `minBytes` | `1` | New bytes required to arm the wake. Raise it to ignore low-value chatter on a noisy job |

```yaml
- set:
    - id: job-output-wake
      config:
        graceMs: 1000
        minBytes: 1
```

## What it does not do

- It does **not** kill or cancel the background job; the job keeps running and a
  later `job_output` read returns its state.
- It does **not** re-arm the wait. The model decides whether to wait again, with
  its own `timeout_ms`.
- It does **not** touch the steer trigger — core owns that.
- It does **not** wake a wait whose job writes nothing: such a call blocks to
  settlement or its timeout exactly as before.
- It replicates `job_output`'s own delta rendering (`stdout`, one `[stderr]`
  section, dropped-output notice) so a wake reads like an ordinary read. If core
  changes that renderer, `renderModelDelta` in `index.js` is the only place to
  follow it.
- A wake is reported with `isError: true` and code `JOB_OUTPUT_RESUMED`, so a
  consumer that treats every tool error as a failure will see this as one; the
  model-facing text always explains what happened.

## Verification

Live, in the desktop profile with this bundle installed and active:

- A job that printed a line 3 s into a 30 s blocking wait returned **early**,
  after the 1 s grace, with `[status: running]` plus "produced 23 bytes of new
  output" — 23 being exactly the appended line. The wait did not run to its cap.
- The same call on a job that printed nothing new blocked to its own `timeout_ms`
  and returned the tool's plain result, unchanged.
- `test/run.mjs` covers the trigger, the `minBytes` floor, the grace-wins path,
  pre-existing output, delegation, and delta rendering: 9/9 pass.

Behaviour checked against the installed **dsh 0.2.2-rc.3** sources:

- `@deepseek-ai/dsh-tools` `src/events.ts` — `tools/execute` still a waterfall
  over `(exec, next)` with a mutable `exec.signal`, and `src/pipeline.ts`
  (`normalizeDispatchResult`) passes an `isError` wrapper result through with its
  `content` and `error`.
- `@deepseek-ai/dsh-tool-jobs` `src/index.ts` — `job_output`'s `wait: true` branch
  (core's own steer release), `jobs.read`/`jobs.wait` call signatures, and the
  `{text, job}` output contract.
- `@deepseek-ai/dsh-jobs` `src/types.ts` + `src/view.ts` — the `output` event,
  `JobId`-keyed reads, `JobView.output.total`, and `JobChunk` channels.
- `@deepseek-ai/dsh-jobs-local` `src/index.ts` — `output` emitted only on a real
  append, `wait` rejecting `AbortError` on abort, `read` consuming the model
  cursor, and `{ owner }`/`{ owners: 'all' }` event filters.

## Attribution and license

MIT — see [LICENSE](LICENSE). This package is a **derivative work** of
[`@argszero/cordis-plugin-steer-preempt`](https://github.com/argszero/cordis-plugin-steer-preempt)
(MIT, © 2026 argszero), and that copyright notice is retained in the LICENSE.

What came from the original: the approach of preempting a blocking
`job_output(wait: true)` from a `tools/execute` around-wrapper by swapping a
derived `AbortController` signal into `exec.signal`, and the `JOB_OUTPUT_RESUMED`
structured-result delivery. What this port changed: the steer trigger is dropped
because dsh core now implements it; the output trigger is rebuilt on the `output`
event and `jobs.get` instead of a polling consuming read; configuration is
reduced to `graceMs`, `minBytes` and `enabled`; and the package carries no build
step or dependencies.

`renderModelDelta` follows the rendering of `@deepseek-ai/dsh-tool-jobs`
(`src/render.ts`, MIT, © DeepSeek) so a wake reads exactly like an ordinary
`job_output` read. `statusLine` follows the same file.
