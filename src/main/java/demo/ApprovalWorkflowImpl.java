package demo;

import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import org.slf4j.Logger;

public class ApprovalWorkflowImpl implements ApprovalWorkflow {

  private static final Logger log = Workflow.getLogger(ApprovalWorkflowImpl.class);

  private boolean approved = false;
  // Only used by the RACE modes: a promise that completes when the condition becomes true.
  private final CompletablePromise<Void> approvedPromise = Workflow.newPromise();

  @Override
  public void approve() {
    log.info("Signal received: approved = true");
    approved = true;
    approvedPromise.complete(null);
  }

  @Override
  public String run(Mode mode, int timeoutSeconds) {
    Duration timeout = Duration.ofSeconds(timeoutSeconds);
    switch (mode) {
      case AWAIT:
        return awaitWithTimeout(timeout);
      case RACE:
        return raceWithoutCancel(timeout);
      case RACE_WITH_CANCEL:
        return raceWithCancel(timeout);
      default:
        throw new IllegalArgumentException("Unknown mode " + mode);
    }
  }

  /**
   * Idiomatic approach. Workflow.await(Duration, Supplier) starts a durable timer
   * (TimerStarted). If the condition becomes true first, the SDK cancels the timer
   * (TimerCanceled) and returns true. If the timer fires first (TimerFired), it returns false.
   */
  private String awaitWithTimeout(Duration timeout) {
    log.info("AWAIT: waiting up to {} for approval", timeout);
    boolean conditionMet = Workflow.await(timeout, () -> approved);
    String result = conditionMet ? "AWAIT: approved before timeout" : "AWAIT: timed out";
    log.info(result);
    return result;
  }

  /**
   * Manual race without cancellation. Whichever promise completes first unblocks us,
   * but the timer is NOT cancelled - it keeps running on the server. If the workflow
   * is still open when it expires, you will see TimerFired in history even though
   * nobody is listening any more. (If the workflow completes first, the timer is
   * simply discarded with the workflow.)
   */
  private String raceWithoutCancel(Duration timeout) {
    log.info("RACE: waiting up to {} for approval (timer will NOT be cancelled)", timeout);
    Promise<Void> timer = Workflow.newTimer(timeout);
    Promise.anyOf(timer, approvedPromise).get();
    String result = approved ? "RACE: approved before timeout" : "RACE: timed out";
    log.info(result);

    // Simulate more work after the race so the stale timer has a chance to fire.
    log.info("RACE: doing follow-up work for {} ...", timeout);
    Workflow.sleep(timeout);
    log.info("RACE: follow-up work done; was original timer fired? {}", timer.isCompleted());
    return result;
  }

  /**
   * Manual race with explicit cancellation. The timer is created inside a
   * CancellationScope; once the condition wins, we cancel the scope, which records
   * TimerCanceled - the same outcome Workflow.await(timeout, cond) gives you for free.
   */
  private String raceWithCancel(Duration timeout) {
    log.info("RACE_WITH_CANCEL: waiting up to {} for approval", timeout);
    Promise<Void>[] timerHolder = new Promise[1];
    CancellationScope timerScope =
        Workflow.newCancellationScope(() -> timerHolder[0] = Workflow.newTimer(timeout));
    timerScope.run();

    Promise.anyOf(timerHolder[0], approvedPromise).get();
    if (approved) {
      log.info("RACE_WITH_CANCEL: condition won, cancelling the timer");
      timerScope.cancel();
    }
    String result =
        approved ? "RACE_WITH_CANCEL: approved before timeout" : "RACE_WITH_CANCEL: timed out";
    log.info(result);

    // Same follow-up work as RACE, to prove the cancelled timer never fires.
    log.info("RACE_WITH_CANCEL: doing follow-up work for {} ...", timeout);
    Workflow.sleep(timeout);
    return result;
  }
}
