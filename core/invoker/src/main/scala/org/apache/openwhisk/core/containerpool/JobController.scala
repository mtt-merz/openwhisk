package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.common.Logging
import spray.json.{JsNumber, JsString}

/**
 * Binds each XActor instance to a ContainerId (different instances can be bound to the same container).
 * In this way, the instance can always be ran on the same container.
 * Additionally, keeps track of the last executed request offset in the container.
 *
 * @param id is the actor instance identifier
 * @param kind is the actor instance class
 */
case class JobController(private var id: String,
                         private var kind: String,
                         var containerId: Option[ContainerId] = Option.empty)(implicit val logging: Logging) {

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

    println(s"\nRequest ${job.offset} STARTED running\n")

    val cId = container.containerId
    if (containerId.isEmpty) containerId = Option(cId)
    else if (containerId.get != cId)
      throw new BindingException(cId)
  }

  private val printActor = s"${kind.capitalize}@$id"

  /**
   * Reset the controller binding.
   *
   * @throws UnBindingException if the XActor is already unbound
   */
  private def unBind(): Unit = {
    if (!isBound) throw new UnBindingException()
    containerId = Option.empty
    lastExecutedOffset = snapshotOffset
    isRestoring = true
    println(s"\nController unbound for $printActor\n")
  }

  private class BindingException(cId: ContainerId)
      extends Exception(s"$printActor operations should be ran on ${containerId.get}, not on $cId") {}

  private class UnBindingException() extends Exception(s"$printActor is not bound") {}
}

object JobController {
  private var controllers: Map[(String, String), JobController] = Map()

  var offset: Long = -1

  def of(job: Run)(implicit logging: Logging): JobController = {
    val id = job.msg.getContentField("id").asInstanceOf[JsString].value
    val kind = job.msg.action.name.asString
    val key = (id, kind)
    controllers.getOrElse(key, {
      val binding = new JobController(id, kind)
      controllers += (key -> binding)
      logging.info(this, s"Controller initialized for ${binding.printActor}")

      binding
    })
  }

  def onExecutionStarted(job: Run, container: Container)(implicit logging: Logging): Unit =
    JobController.of(job).onExecutionStarted(container)(job)

  def onExecutionFailed(job: Run): Unit = {
    assert(offset == job.offset)
    offset -= 1
  }

  def onExecutionFinished(job: Run)(implicit logging: Logging): Unit =
    JobController.of(job).onExecutionFinished(job)

  /**
   * Un-bind the controllers bound to the given container and calculate the offset to restore.
   *
   * @param container defines the controllers to un-bind.
   * @return the offset to restore.
   */
  def removeContainer(container: Container): Option[Long] = {
    val containerId = Option(container.containerId)
    println("REMOVE CONTAINER METHOD")
    var offsets: List[Long] = List.empty
    controllers.values.foreach {
      case controller @ JobController(_, _, `containerId`) =>
        controller.unBind()
        offsets = offsets :+ controller.snapshotOffset
      case _ =>
    }
    if (offsets.isEmpty) Option.empty
    else {
      offset = offsets.min - 1
      Option(offset + 1)
    }
  }

  /**
   * Check if the request should be executed or not.
   * This is true if
   * (1) the request has never been executed or
   * (2) the request should be re-executed to restore the container state.
   *
   * @param job is the Run instance to check
   * @return true if it should be executed, false otherwise.
   */
  def checkIfAlreadyExecuted(job: Run)(implicit logging: Logging): Boolean = {
    if (job.offset == offset + 1) offset += 1

    val controller = of(job)

    // Offset of the currently running request, if any, or of the last executed request
//    val currentOffset = controller.runningOffset.getOrElse(controller.lastExecutedOffset)
    if (job.offset >= offset) {
      print(
        s"\nOK: Currently running/last executed request $offset is BEFORE the received request ${job.offset}\n")
      true
    } else {
      print(
        s"\nREQUEST SKIPPED: Received request ${job.offset} is BEFORE LAST request $offset\n")
      false
    }
  }

  /**
   * Ensure the new request has been received in the correct order (it is not guaranteed by OpenWhisk).
   * If the order is not correct, the request should be enqueued in the buffer.
   *
   * While the requests are processed in order till the InvokerReactive level, they arrives to the ContainerPool
   * in a different order. It depends on the time needed by each request to fetch the action code from the DB.
   *
   * @param job is the Run instance to check.
   * @return true if the order is correct, false otherwise.
   */
  def checkOrder(job: Run): Boolean = {
    if (job.offset == offset) {
      println(s"The order is correct: job.offset (${job.offset}) == receivedOffset ($offset)")
      true
    } else {
      println(s"The order is NOT correct: job.offset (${job.offset}) != receivedOffset ($offset)")
      false
    }
  }

  implicit class ContainerDataChecker(data: ContainerData)(implicit val logging: Logging) {

    /**
     * Check if the container is supposed to run the request.
     * This is true if
     * (1) the related XActor is not yet bound to any container or
     * (2) the related XActor is bound to THIS container.
     *
     * @param job is the request.
     * @return true if the request can be ran, false otherwise.
     */
    def canRun(job: Run): Boolean = {
      val controller = of(job)

      if (controller.containerId.isEmpty || data.getContainer.isEmpty) true
      else data.getContainer.get.containerId == controller.containerId.get
    }
  }

  implicit class JobBufferEnhancer(buffer: JobBuffer)(implicit val logging: Logging) {
    def hasNothingToExecute: Boolean = {
      if (buffer.map(_.offset).contains(offset)) {
        println(s"Buffer contains $offset, so it has SOMETHING ELSE to execute before")
        false
      } else {
        println(s"Buffer does NOT contain $offset, so it has NOTHING ELSE to execute before")
        true
      }

    }
  }
}
