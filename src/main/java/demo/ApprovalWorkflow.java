package demo;

import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface ApprovalWorkflow {

  /**
   * Waits for an approval signal OR a timeout, whichever comes first.
   *
   * @param mode           which waiting technique to use (see {@link Mode})
   * @param timeoutSeconds how long the timer runs
   * @return a description of what happened
   */
  @WorkflowMethod
  String run(Mode mode, int timeoutSeconds);

  @SignalMethod
  void approve();

  enum Mode {
    /** Workflow.await(timeout, condition) - SDK creates the timer and cancels it for you. */
    AWAIT,
    /** Manual race: Promise.anyOf(timer, conditionPromise) - timer is NOT cancelled. */
    RACE,
    /** Manual race inside a CancellationScope - we cancel the timer ourselves. */
    RACE_WITH_CANCEL
  }
}
