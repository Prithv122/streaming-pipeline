# Interview Prep — streaming-pipeline

**Five questions, five answers.** An unanswered question means this project is not shipped.

---

### Q1. Walk me through the architecture in 90 seconds.

**A:**
- **The flow.** An order stream is cut into 60-second tumbling windows per zone. Events carry an
  event time and an arrival time, and a seeded generator makes a known share of them late.
- **The core.** A hand-written windowing core tracks `M`, the latest event time seen. A window is
  *due* when `M` passes its end, and *closed* when `M` passes end + `L`, where `L` is the lateness
  budget. Events for a closed window are dropped and counted.
- **Two policies on the same core.**
  - *Wait* sends one result at close.
  - *Revise* sends a provisional result when the window is due, a correction for each late event,
    and a final record at close.
- **Scoring.** A simulator replays the log with arrival time as the clock and scores every window
  against a batch recompute of the same log. The same core also runs behind a Redpanda topic, and a
  bridge pushes results to the WebSocket dashboard from my project #29.

### Q2. Why hand-write the windowing instead of using Flink or Bytewax?

**A:**
- **The semantics were the point.** The result depends entirely on watermark and lateness rules.
  The subtle cases are an event that arrives exactly when the watermark passes, a window that becomes
  due and closes in the same step, and a late event that creates a window. A framework decides
  those for you and needs a broker to test.
- **Owning the 166-line core bought three things:**
  - unit tests with hand-derived fixtures;
  - a literal-spec oracle to compare against;
  - an analytic model that predicts the core's behaviour.
- **In production I'd use Flink.** Its allowed-lateness feature is the revise policy here, plus
  checkpointed state.

### Q3. What's the weakest part, and what breaks first under load?

**A:**
- **One partition.** The watermark is global, so the consumer needs one total order across zones.
  At scale you partition by zone, keep a watermark per partition, and take the minimum. An idle
  partition then stalls every window, so you need idle-source detection.
- **State is in memory.** A crash loses open windows. A replay from the committed offset
  double-counts unless the output is transactional or downstream upserts are idempotent by
  `(zone, window_start, revision)`.
- **The lateness model is synthetic.** Uniform delays, no correlated bursts of lateness. Real
  delays are heavy-tailed, so `L` should be set from the observed delay distribution.

### Q4. How do you know it works? What did you measure, against what baseline?

**A:**
- **Three layers of evidence.**
  - Unit and property tests compare the core with a slow literal-spec oracle.
  - Every window is scored against a batch recompute of the same log.
  - A pre-registered sweep tested 450 runs.
- **The original claim could not fail.** I froze "L = 30 s gives ≥ 99 % exact windows at 5 % late
  / ≤ 30 s delay". An independent review before the run showed it was guaranteed: delay ≤ 30 = L
  means nothing is ever dropped. I kept it as a construction check and amended the plan before any
  data.
- **The new claim is falsifiable.** I derived the core's behaviour from the generator's Poisson
  process: drop rate, first-answer exactness, final exactness, first-emit latency. I froze
  predictions with exact acceptance regions and pinned the file's hash.
- **Result: 369/369 checks passed.**
- **The baseline is `L = 0`**: emit at the watermark, drop everything late. At 5 % late / ≤ 30 s,
  only 39.9 % of windows are exact and 1.6 % of events are dropped. `L = 30` gives 100 %, with the
  answer about 31.6 s after the window ends.

### Q5. If revise gets the same final answer sooner, why would anyone choose wait?

**A:**
- **The final answers are identical by construction.** Both policies accept exactly the same events,
  and the tests pin that.
- **What revise buys:** a first answer about 1.5 s after the window ends instead of about `L` + 1.5 s.
- **What it costs, at 5 % late / ≤ 30 s delay:**
  - that first answer is exact for only 39.9 % of windows;
  - at `L = 30` each window gets 0.92 value-changing corrections on average;
  - it sends 2.9× the records of wait (1,044 vs 360 per run).
- **In the stress regime** (20 % late, up to 120 s, `L = 60`) it is 6.1 corrections per window and
  8× the records.
- **So:**
  - choose wait when downstream acts on the number once, as billing or alerting does;
  - choose revise when a reader can tolerate a number that changes, as a live dashboard can, and can
    absorb the correction traffic;
  - coalesce revisions at the hub when there are many subscribers.

---

## 30-second pitch

Every streaming job has to decide how long to wait for late events. I built a windowing core with
one lateness budget and two policies: wait for completeness, or answer early and correct. I scored
both against a batch recompute. My first pre-registered claim turned out to be true by
construction, so I replaced it with an analytic model of the generator that predicts drops,
accuracy and latency, and froze it before running. It passed all 369 checks. At 5 % late events,
answering early gets a number out about 30 seconds sooner, but that number is exact only 40 % of the
time and needs about one correction per window.
