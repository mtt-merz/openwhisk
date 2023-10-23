package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.entity.ActivationResponse
import spray.json.JsString

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
   * Update the  offset and eventually the snapshot offset
   */
  private def onExecutionSuccess(runInterval: Interval, response: ActivationResponse)(implicit job: Run): Unit = {
    RunLogger.execution(job, runInterval)
    RunLogger.result(job, runInterval, response)
  }
}

object RunController {
  private var controllers: Map[(String, String), RunController] = Map()

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

  implicit class ImplicitRun(run: Run)(implicit val logging: Logging) {
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
      RunLogger.arrival(run)
      true
    }
  }
}
