package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.common.{Logging, TransactionId}
import spray.json.{JsNumber, JsString}

/**
 * Binds each XActor instance to a ContainerId (different instances can be bound to the same container).
 * In this way, the instance can always be ran on the same container.
 * Additionally, keeps track of the last executed request offset in the container.
 *
 * @param id is the actor instance identifier
 * @param kind is the actor instance class
 */
case class RunController(private var id: String,
                         private var kind: String,
                         var containerId: Option[ContainerId] = Option.empty)(
  implicit val logging: Logging,
  implicit val transactionId: TransactionId) {

  private var runningOffset: Option[Long] = Option.empty
  private var lastExecutedOffset: Long = -1
  private var snapshotOffset: Long = 0

  private var isRestoring: Boolean = false

  /**
   * Check if the controller is bound to a ContainerId
   *
   * @return true if it is bound, false otherwise.
   */
  def isBound: Boolean = containerId.isDefined

  /**
   * Update the  offset and eventually the snapshot offset
   */
  private def onExecutionFinished(implicit job: Run): Unit = {
    assert(runningOffset.isDefined && runningOffset.get == job.offset)
    runningOffset = Option.empty
    lastExecutedOffset = job.msg.getContentField("offset").asInstanceOf[JsNumber].value.toLong
  }

  /**
   * Bind a container to the controller, if not currently bound; if already bound,
   * check that the container is the correct one.
   *
   * @param container is the container to check
   * @throws BindingException if there is a bound container but it is not the received one
   */
  private def onExecutionStarted(container: Container)(implicit job: Run): Unit = {
    assert(runningOffset.isEmpty)
    runningOffset = Option(job.offset)

    val cId = container.containerId
    if (containerId.isEmpty) {
      containerId = Option(cId)
      logging.info(this, s"$printActor BOUND to $cId")
    } else if (containerId.get != cId)
      throw new BindingException(cId)
  }

  private val printActor = s"${kind.capitalize}@$id"

  /**
   * Reset the controller binding.
   *
   * @throws UnBindingException if the controller is already unbound
   */
  private def unBind(): Unit = {
    if (!isBound) throw new UnBindingException()

    logging.info(this, s"$printActor UN-BOUND from ${containerId.get}")
    containerId = Option.empty
    lastExecutedOffset = snapshotOffset - 1
    isRestoring = true
  }

  private class BindingException(cId: ContainerId)
      extends Exception(s"$printActor operations should be ran on ${containerId.get}, not on $cId") {}

  private class UnBindingException() extends Exception(s"$printActor is not bound") {}
}

object RunController {
  private var controllers: Map[(String, String), RunController] = Map()
  private var globalOffset: Long = -1

  def of(run: Run)(implicit logging: Logging): RunController = {
    implicit val transactionId: TransactionId = run.msg.transid

    val id = run.msg.getContentField("actor_id").asInstanceOf[JsString].value
    val kind = run.msg.action.name.asString

    val key = (id, kind)
    controllers.getOrElse(key, {
      val binding = new RunController(id, kind)
      controllers += (key -> binding)
      logging.info(this, s"Controller initialized for ${binding.printActor}")

      binding
    })
  }

  def onExecutionStarted(run: Run, container: Container)(implicit logging: Logging): Unit =
    RunController.of(run).onExecutionStarted(container)(run)

  def onExecutionFailed(): Unit = globalOffset -= 1

  def onExecutionFailed(run: Run): Unit = {
    assert(globalOffset == run.offset)
    onExecutionFailed()
  }

  def onExecutionFinished(run: Run)(implicit logging: Logging): Unit =
    RunController.of(run).onExecutionFinished(run)

  /**
   * Un-bind the controllers bound to the given container and calculate the offset to restore.
   *
   * @param container defines the controllers to un-bind.
   * @return the offset to restore.
   */
  def removeContainer(container: Container): Option[Long] = {
    val containerId = Option(container.containerId)

    var offsets: List[Long] = List.empty
    controllers.values.foreach {
      case controller @ RunController(_, _, `containerId`) =>
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
     * @param run is the request.
     * @return true if the request can be executed in this container, false otherwise.
     */
    def canExecute(run: Run): Boolean = {
      val controller = of(run)

      if (controller.containerId.isEmpty || data.getContainer.isEmpty) true
      else data.getContainer.get.containerId == controller.containerId.get
    }
  }

//  implicit class ImplicitJobBuffer(buffer: RunBuffer)(implicit val logging: Logging) {
//
//    /**
//     * Check if the request that should be executed next is already in the buffer
//     *
//     * @return true if the buffer contains this request, false otherwise
//     */
//    def hasNothingToExecute: Boolean = !buffer.map(_.offset).contains(globalOffset)
//  }

  implicit class ImplicitRun(run: Run)(implicit val logging: Logging) {
    implicit val transactionId: TransactionId = run.msg.transid

    /**
     * Ensure the request is the next one to be executed.
     *
     * @return true if the order is correct, false otherwise.
     */
    def canBeExecutedNext: Boolean = run.offset == globalOffset + 1

    /**
     * Ensure the request has been received in the correct order (it is not guaranteed by OpenWhisk).
     * If the order is not correct, the request should be enqueued in the buffer.
     *
     * While the requests are processed in order till the InvokerReactive level, they arrives to the ContainerPool
     * in a different order. It depends on the time needed by each request to fetch the action code from the DB.
     *
     * @return true if the order is correct, false otherwise.
     */
    def canBeExecutedNow: Boolean = run.offset == globalOffset

    /**
     * Check if the job should be executed or not.
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

      // Update global offset
      if (run.offset == globalOffset + 1) globalOffset += 1

      val controller = of(run)

      // Offset of the currently running request, if any, or of the last executed request
      val currentOffset = controller.runningOffset.getOrElse(controller.lastExecutedOffset)
      run.offset > currentOffset
    }
  }
}
