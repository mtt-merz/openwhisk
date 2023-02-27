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
case class XActorController(var id: String,
                            var kind: String,
                            var containerId: Option[ContainerId] = Option.empty,
                            var offset: Long = 0,
                            var snapshotOffset: Long = 0)(implicit val logging: Logging) {
  private val actorString = s"${kind.capitalize}@$id"

  private class BindingException(cId: ContainerId)
      extends Exception(s"$actorString operations should be ran on ${containerId.get}, not on $cId") {}

  private class UnBindingException() extends Exception(s"$actorString is not bound") {}

  /**
   * Check if the XActor instance is bounded to a ContainerId
   *
   * @return the result of the checking operation
   */
  def isBound: Boolean = containerId.isDefined

  /**
   * Bind a container to the XActor, if not currently bound.
   * If already bound, check that the container is the correct one.
   *
   * @param container is the container to check
   * @throws BindingException if there is a bound container but it is not the received one
   */
  def bind(container: Container): Unit = {
    val cId = container.containerId
    if (containerId.isEmpty) containerId = Option(cId)
    else if (containerId.get != cId)
      throw new BindingException(cId)
  }

  /**
   * Reset the XActor binding.
   *
   * @throws UnBindingException if the XActor is already unbound
   */
  private def unBind(): Unit = {
    if (!isBound) throw new UnBindingException()
    containerId = Option.empty
    offset = snapshotOffset
    logging.info(this, s"Controller unbound for $actorString")
  }

  /**
   * Update the  offset and eventually the snapshot offset
   */
  def update(r: Run): Unit = {
    offset = r.msg.getContentField("offset").asInstanceOf[JsNumber].value.toLong
  }
}

object XActorController {
  private var controllers: Map[(String, String), XActorController] = Map()

  def of(r: Run)(implicit logging: Logging): XActorController = {
    val id = r.msg.getContentField("id").asInstanceOf[JsString].value
    val kind = r.msg.action.name.asString

    val key = (id, kind)
    controllers.getOrElse(key, {
      val binding = new XActorController(id, kind)
      controllers += (key -> binding)
      logging.info(this, s"Controller initialized for ${binding.actorString}")

      binding
    })
  }

  def removeContainer(container: Container): Unit = {
    val containerId = container.containerId
    controllers.values.foreach {
      case controller @ XActorController(_, _, `containerId`, _, _) =>
        controller.unBind()
      case _ =>
    }
  }

  implicit class ContainerDataChecker(data: ContainerData)(implicit val logging: Logging) {

    /**
     * Check if the container is supposed to run the request.
     * This is true if
     * (1) the related XActor is not yet bound to any container or
     * (2) the related XActor is bound to THIS container.
     *
     * @param r is the request.
     * @return true if the request can be ran, false otherwise.
     */
    def canRun(r: Run): Boolean = {
      val controller = of(r)

      if (controller.containerId.isEmpty || data.getContainer.isEmpty) true
      else data.getContainer.get.containerId == controller.containerId.get
    }
  }

  implicit class RunChecker(r: Run)(implicit val logging: Logging) {

    /**
     * Check if the request should be executed or not.
     * This is true if
     * (1) the request has never been executed or
     * (2) the request should be re-executed to restore the container state.
     *
     * @return the result of the check.
     */
    def shouldBeExecuted: Boolean = {
      val controller = of(r)

      val requestOffset = r.msg.getContentField("offset").asInstanceOf[JsNumber].value
      requestOffset > controller.offset
    }
  }

}
