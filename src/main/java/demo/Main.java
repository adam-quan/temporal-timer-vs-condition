package demo;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.time.Duration;
import java.time.Instant;

/**
 * Usage: mvn compile exec:java -Dexec.args="<MODE> <timeoutSeconds> <signalAfterSeconds|none>"
 *
 * Examples:
 *   AWAIT 10 3            -> condition wins, timer is cancelled
 *   AWAIT 5 none          -> timer wins
 *   RACE 10 3             -> condition wins, timer is NOT cancelled and fires later
 *   RACE_WITH_CANCEL 10 3 -> condition wins, we cancel the timer ourselves
 */
public class Main {

  static final String TASK_QUEUE = "timer-vs-condition";

  public static void main(String[] args) throws Exception {
    ApprovalWorkflow.Mode mode =
        ApprovalWorkflow.Mode.valueOf(args.length > 0 ? args[0] : "AWAIT");
    int timeoutSeconds = args.length > 1 ? Integer.parseInt(args[1]) : 10;
    Integer signalAfter =
        args.length > 2 && !args[2].equalsIgnoreCase("none") ? Integer.parseInt(args[2]) : null;

    // Connect to local dev server (temporal server start-dev)
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    // Run worker in-process for convenience
    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(ApprovalWorkflowImpl.class);
    factory.start();

    String workflowId = "timer-demo-" + mode + "-" + System.currentTimeMillis();
    ApprovalWorkflow wf =
        client.newWorkflowStub(
            ApprovalWorkflow.class,
            WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());

    System.out.printf(
        "%n=== Starting %s | mode=%s timeout=%ds signal=%s ===%n%n",
        workflowId, mode, timeoutSeconds, signalAfter == null ? "never" : "after " + signalAfter + "s");
    WorkflowExecution execution = WorkflowClient.start(wf::run, mode, timeoutSeconds);

    if (signalAfter != null) {
      Thread.sleep(Duration.ofSeconds(signalAfter).toMillis());
      System.out.println(">>> Client sending 'approve' signal");
      wf.approve();
    }

    String result = WorkflowStub.fromTyped(wf).getResult(String.class);
    System.out.printf("%n=== Result: %s ===%n", result);

    printTimerEvents(client, execution.getWorkflowId());

    factory.shutdown();
    service.shutdown();
    System.exit(0);
  }

  /** Print the event history, highlighting timer events. */
  private static void printTimerEvents(WorkflowClient client, String workflowId) {
    WorkflowExecutionHistory history = client.fetchHistory(workflowId);
    System.out.println("\n=== Event history ===");
    Instant start = null;
    for (HistoryEvent e : history.getEvents()) {
      Instant t = Instant.ofEpochSecond(e.getEventTime().getSeconds(), e.getEventTime().getNanos());
      if (start == null) start = t;
      String type = e.getEventType().name().replace("EVENT_TYPE_", "");
      String detail = "";
      switch (e.getEventType()) {
        case EVENT_TYPE_TIMER_STARTED:
          var s = e.getTimerStartedEventAttributes();
          detail = "timerId=" + s.getTimerId() + " duration=" + s.getStartToFireTimeout().getSeconds() + "s";
          break;
        case EVENT_TYPE_TIMER_FIRED:
          detail = "timerId=" + e.getTimerFiredEventAttributes().getTimerId();
          break;
        case EVENT_TYPE_TIMER_CANCELED:
          detail = "timerId=" + e.getTimerCanceledEventAttributes().getTimerId();
          break;
        case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
          detail = "signal=" + e.getWorkflowExecutionSignaledEventAttributes().getSignalName();
          break;
        default:
      }
      boolean highlight = type.startsWith("TIMER") || type.contains("SIGNALED");
      System.out.printf(
          "%s %3d  +%5.1fs  %-35s %s%n",
          highlight ? "*" : " ",
          e.getEventId(),
          Duration.between(start, t).toMillis() / 1000.0,
          type,
          detail);
    }
  }
}
