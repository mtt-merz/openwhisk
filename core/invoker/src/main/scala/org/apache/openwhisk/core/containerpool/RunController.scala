package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.entity.ActivationResponse
import spray.json.{JsBoolean, JsNumber, JsString}

import java.util.{Calendar, Date}

/**
 * Binds each XActor instance to a ContainerId (different instances can be bound to the same container).
 * In this way, the instance can always be ran on the same container.
 *
 * Additionally, keeps track of the last executed request offset in the container.
 *
 * @param id the actor instance identifier
 * @param kind the actor instance class
 */
case class RunController(private val id: String,
                         private val kind: String,
                         var boundContainerId: Option[ContainerId] = Option.empty,
                         private var restoringTargetOffset: Option[Long] = Option.empty,
                         private var runningOffset: Option[Long] = Option.empty,
                         private var lastExecutedOffset: Long = -1,
                         private var snapshotOffset: Long = 0,
                         private var snapshotTimestamp: Date = Calendar.getInstance().getTime)(
  implicit val logging: Logging,
  implicit val transactionId: TransactionId) {

  private val label = s"${kind.capitalize}@$id"

  /**
   * Check if the controller is bound to a ContainerId
   *
   * @return true if it is bound, false otherwise.
   */
  def isBound: Boolean = boundContainerId.isDefined

  /**
   * Bind the received container to the controller.
   *
   * @param container the container to bind
   */
  private def bind(container: Container): Unit = {
    val cId = container.containerId

    boundContainerId = Option(cId)
    logging.info(this, s"$label BOUND to $cId")
  }

  /**
   * Reset the controller binding.
   *
   * @throws UnBindingException if the controller is already unbound
   */
  private def unBind(): Unit = {
    if (!isBound) throw new UnBindingException()

    logging.info(this, s"$label UN-BOUND from ${boundContainerId.get}")
    boundContainerId = Option.empty

    restoringTargetOffset = Option(lastExecutedOffset)
    lastExecutedOffset = snapshotOffset - 1
  }

  private def refine()(implicit job: Run): Run =
    job.copy(msg = {
      var msg = job.msg
      msg = msg.addContentField("persist", JsBoolean(shouldPerformSnapshot))
      msg = msg.addContentField("isolate", JsBoolean(isRestoring))

      msg
    })

  private val snapshotActionsCount = 10
  private val snapshotTimeInterval = 500 // milliseconds

  /**
   * Check if there is the need to perform a snapshot.
   * This is true if
   *
   *   (1) the time elapsed since the last snapshot is greater or equal to the threshold
   *
   *   (2) the executed actions count since the last snapshot is greater or equal to the threshold
   *
   * @return true if the snapshot should be performed, false otherwise.
   */
  private def shouldPerformSnapshot: Boolean = false
//  {
//    val currentDate = Calendar.getInstance().getTime
//    val dateInterval = currentDate.getTime - snapshotTimestamp.getTime
//
//    if (dateInterval >= snapshotTimeInterval) true
//    else if (runningOffset.isEmpty) false
//    else {
//      val actionsCount = runningOffset.get - snapshotOffset
//      actionsCount >= snapshotActionsCount
//    }
//  }

  /**
   * Check if the state is being restored.
   * This is true if [[restoringTargetOffset]] is defined.
   *
   * @return
   */
  private def isRestoring: Boolean = restoringTargetOffset.isDefined

  /**
   * Bind a container to the controller, if not currently bound; if already bound,
   * check that the container is the correct one.
   *
   * @param container the container to check
   * @throws BindingException if there is a bound container but it is not the received one
   */
  private def onExecutionStarted(container: Container)(implicit job: Run): Unit = {
    assert(runningOffset.isEmpty)
    RunLogger.execution(job)

    runningOffset = Option(job.offset)

    val cId = container.containerId
    if (boundContainerId.isEmpty) bind(container)
    else if (boundContainerId.get != cId)
      throw new BindingException(cId)
  }

  /**
   * Update the  offset and eventually the snapshot offset
   */
  private def onExecutionSuccess(runInterval: Interval, response: ActivationResponse)(implicit job: Run): Unit = {
    assert(runningOffset.isDefined && runningOffset.get == job.offset)

    runningOffset = Option.empty
    lastExecutedOffset = job.msg.getContentField("offset").asInstanceOf[JsNumber].value.toLong

    if (isRestoring && lastExecutedOffset == restoringTargetOffset.get)
      restoringTargetOffset = Option.empty

    if (shouldPerformSnapshot) {
      snapshotOffset = job.offset
      snapshotTimestamp = Calendar.getInstance().getTime
    }

    RunLogger.result(job, runInterval, response)
  }

  private class BindingException(cId: ContainerId)
      extends Exception(s"$label operations should be ran on ${boundContainerId.get}, not on $cId") {}

  private class UnBindingException() extends Exception(s"$label is not bound") {}
}

object RunController {
  private var controllers: Map[(String, String), RunController] = Map()
  private var globalOffset: Long = -1

  def of(run: Run)(implicit logging: Logging): RunController = {
    implicit val transactionId: TransactionId = run.msg.transid

    val name = run.msg.getContentField("actor_name").asInstanceOf[JsString].value
    val kind = run.msg.action.name.asString

    val key = (name, kind)
    controllers.getOrElse(key, {
      val binding = new RunController(name, kind)
      controllers += (key -> binding)
      logging.info(this, s"Controller initialized for ${binding.label}")

      binding
    })
  }

  def refine(run: Run)(implicit logging: Logging): Run =
    of(run).refine()(run)

  def onExecutionStart(run: Run, container: Container)(implicit logging: Logging): Unit =
    of(run).onExecutionStarted(container)(run)

  def onExecutionFailure(): Unit = globalOffset -= 1

  def onExecutionFailure(run: Run): Unit = {
    assert(globalOffset == run.offset)
    onExecutionFailure()
  }

  def onExecutionSuccess(run: Run, runInterval: Interval, response: ActivationResponse)(
    implicit logging: Logging): Unit =
    of(run).onExecutionSuccess(runInterval, response)(run)

  /**
   * Un-bind the controllers bound to the given container and calculate the offset to restore.
   *
   * @param container the controllers to un-bind.
   * @return the offset to restore.
   */
  def removeContainer(container: Container): Option[Long] = {
    val containerId = Option(container.containerId)

    var offsets: List[Long] = List.empty
    controllers.values.foreach {
      case controller @ RunController(_, _, `containerId`, _, _, _, _, _) =>
        controller.unBind()
        offsets = offsets :+ controller.snapshotOffset
      case _ =>
    }
    if (offsets.isEmpty) Option.empty
    else {
      globalOffset = offsets.min - 1
      Option(globalOffset + 1)
    }
  }

  implicit class ImplicitContainerData(data: ContainerData)(implicit val logging: Logging) {

    /**
     * Check if the container is supposed to run the request.
     * This is true if
     * (1) the related controller is not yet bound to any container or
     * (2) the related controller is bound to THIS container.
     *
     * @param run the request.
     * @return true if the request can be executed in this container, false otherwise.
     */
    def canExecute(run: Run): Boolean = {
      val controller = of(run)

      if (controller.boundContainerId.isEmpty || data.getContainer.isEmpty) true
      else data.getContainer.get.containerId == controller.boundContainerId.get
    }
  }

  implicit class ImplicitRun(run: Run)(implicit val logging: Logging) {
    implicit val transactionId: TransactionId = run.msg.transid

    /**
     * Ensure the request has been received in the correct order (it is not guaranteed by OpenWhisk).
     * If the order is not correct, the request should be enqueued in the buffer.
     *
     * While the requests are processed in order till the InvokerReactive level, they arrives to the ContainerPool
     * in a different order. It depends on the time needed by each request to fetch the action code from the DB.
     *
     * @return true if the order is correct, false otherwise.
     */
    def canBeExecutedNow: Boolean = run.offset <= globalOffset

//    /**
//     * Ensure the request can be executed next.
//     *
//     * @return true if the order is correct, false otherwise.
//     */
//    def canBeExecutedNext: Boolean = run.offset <= globalOffset + 1

    /**
     * Check if the job should be executed.
     * This is true if
     * (1) the request has never been executed or
     * (2) the request should be re-executed to restore the container state.
     *
     * This method is called every time a new request arrives, so it is also used to increment the global offset
     * and to log the request arrival
     *
     * @return true if it should be executed, false otherwise.
     */
    def shouldBeExecuted: Boolean = {
      logging.info(this, s"Received request #${run.offset} -> ${run.msg}")
      RunLogger.arrival(run)

      // Update global offset
      if (run.offset == globalOffset + 1) globalOffset += 1

      val controller = of(run)

      // Offset of the currently running request, if any, or of the last executed request
      val currentOffset = controller.runningOffset.getOrElse(controller.lastExecutedOffset)
      run.offset > currentOffset
    }
  }

// TODO: if the message comes from the queue, even if it is not in order, it could still be executed!
//       If so, the actual ordered job should already be in the queue, bound to a different container.

//  implicit class ImplicitRunBuffer(buffer: RunBuffer) {
//    def shouldEnqueue(job: Run): Boolean = {
//      if (job.offset == globalOffset) true
//      else if (job.offset > globalOffset) false
//      else buffer.lastOffset
//    }
//  }
}
