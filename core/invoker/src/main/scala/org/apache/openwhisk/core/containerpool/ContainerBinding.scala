package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.core.connector.ActivationMessage
import spray.json.JsString

/**
 * An eXternal Actor, it represents the actor instance related to an Action invocation
 *
 * @param id   the instance identifier
 * @param kind the instance class
 */
case class XActor(id: String, kind: String) {
  override def toString: String = s"${kind.capitalize}@$id"
}

object XActor {
  def apply(msg: ActivationMessage): XActor = {
    val id = msg.getContentField("id").asInstanceOf[JsString].value
    val kind = msg.action.name.asString

    new XActor(id, kind)
  }
}

/**
 * Binds each XActor instance to a ContainerId (different instances can be bound to the same container).
 * In this way, the instance is always ran on the same container.
 *
 * @param actor the instance to be bound
 */
class ContainerBinding(var actor: XActor) extends Serializable {
  private var containerId: Option[ContainerId] = Option.empty

  /**
   * Check if the XActor instance is bounded to a ContainerId
   *
   * @return the result of the checking operation
   */
  def isBound: Boolean = containerId.isDefined

  class ContainerBindingException(cId: ContainerId)
      extends Exception(s"$actor operations should be ran on ${containerId.get}, not on $cId") {}

  def bind(c: Container): Unit = {
//    logging.info(
//      this,
//      s"Checking container for $actor" +
//        s"\nDesignated: ${containerId.getOrElse("None")}" +
//        s"\nReceived:   ${c.containerId}")

    val cId = c.containerId
    if (containerId.isEmpty) containerId = Option(cId)
    else if (containerId.get != cId)
      throw new ContainerBindingException(cId)
  }

  private def checkContainerData(data: ContainerData): Boolean = {
    if (containerId.isEmpty || data.getContainer.isEmpty) true
    else data.getContainer.get.containerId == containerId.get
  }
}

object ContainerBinding {
  private var invocationManagers: Map[XActor, ContainerBinding] = Map()

  def apply(actor: XActor): ContainerBinding =
    invocationManagers.getOrElse(actor, {
      val binding = new ContainerBinding(actor)
      invocationManagers += (actor -> binding)
//      logging.info(this, s"ContainerBinding initialized for $actor")

      binding
    })

  def apply(msg: ActivationMessage): ContainerBinding = ContainerBinding(XActor(msg))

  implicit class ContainerDataChecker(data: ContainerData) {
    def canRun(run: Run): Boolean = apply(run.msg).checkContainerData(data)
  }
}
