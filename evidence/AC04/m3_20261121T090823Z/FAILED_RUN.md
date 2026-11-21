# Failed run, kept as evidence

With the watermark generator at parallelism 1 (fix from m3_20261121T090639Z), watermarks still lagged
by about one input burst: the too-late event 05 was accepted into W0 as a late update, the late event
10 was counted in W1 on time, and the last window (18:00Z) never fired. `watermark_at_emit` shows the
lag. Cause: PyFlink returns results from Python operators per "bundle"; here a bundle only ended when
the next elements arrived, so the watermark generator saw each burst only when the next one came, and
never saw the final flush marker. Fix: `python.fn-execution.bundle.size = 1` (configs/streaming.yaml
`python_runtime`) and 6 s pauses between scenario phases. Re-run follows.
