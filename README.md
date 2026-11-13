# Temporal (Java): waiting on a condition AND a timer

Shows what happens to a durable timer when a workflow waits on
"condition OR timeout" and the **condition becomes true first**.

## Run

```bash
temporal server start-dev          # in another terminal (UI: http://localhost:8233)

# Condition wins -> SDK cancels the timer (TimerCanceled)
mvn -q compile exec:java -Dexec.args="AWAIT 10 3"

# Timer wins -> TimerFired, await returns false
mvn -q compile exec:java -Dexec.args="AWAIT 5 none"

# Manual race, no cancel -> timer keeps running and later fires (TimerFired)
mvn -q compile exec:java -Dexec.args="RACE 10 3"

# Manual race + CancellationScope -> TimerCanceled, same as AWAIT
mvn -q compile exec:java -Dexec.args="RACE_WITH_CANCEL 10 3"
```

Args: `<MODE> <timeoutSeconds> <signalAfterSeconds|none>`. Each run prints the
result and the event history, with timer/signal events marked `*`.

## Takeaways

| Technique | Condition first | Timer first |
|---|---|---|
| `Workflow.await(timeout, () -> cond)` | returns `true`, timer is **cancelled** (`TimerCanceled`) | returns `false` (`TimerFired`) |
| `Promise.anyOf(Workflow.newTimer(..), promise)` | unblocks, but timer is **still pending**; fires later if workflow is still open | unblocks (`TimerFired`) |
| Same, timer inside `CancellationScope` + `scope.cancel()` | timer **cancelled** (`TimerCanceled`) | unblocks (`TimerFired`) |

- `Workflow.await(Duration, Supplier<Boolean>)` is the idiomatic way: the SDK
  cleans up the timer for you.
- With a hand-rolled race, an un-cancelled timer is harmless if the workflow
  ends soon after, but in a long-running workflow it leaves a pending timer
  that will still fire (extra history event + workflow task).
