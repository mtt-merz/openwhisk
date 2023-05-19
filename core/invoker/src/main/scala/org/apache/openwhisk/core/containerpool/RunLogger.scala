package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.core.entity.ActivationResponse

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

object RunLogger {
  private val path: String = {
    val path = s"/home/m/Workspaces/thesis/logs/$formattedDateTime"
    new File(path).mkdir()

    path
  }

  private def formattedDateTime: String = {
    val formatter = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss.sss")
    formatter.format(Calendar.getInstance().getTime)
  }

  private def getActorLabel(job: Run): String = {
    val actorType: String = job.msg.action.name.name
    val actorId: String = job.actorId

    s"$actorType@$actorId"
  }

  def arrival(job: Run): Unit = {
    val msg = s"${job.offset} " +
      s"${getActorLabel(job)} ${job.msg.getContentField("message")} " +
      s"${System.currentTimeMillis()}"

    printOnFile(msg, "arrival")
  }

  def execution(job: Run, interval: Interval): Unit = {
    val msg = s"${job.offset} " +
      s"${getActorLabel(job)} ${job.msg.getContentField("message")} " +
      s"${System.currentTimeMillis()} " +
      s"${interval.duration.length}"

    printOnFile(msg, "execution")
  }

  def result(job: Run, interval: Interval, response: ActivationResponse): Unit = {
    val msg = s"${job.offset} " +
      s"${getActorLabel(job)} ${job.msg.getContentField("message")} " +
      s"${System.currentTimeMillis()} " +
      s"${interval.duration.length}\n" +
      s"${response.result.get.prettyPrint}\n"

    printOnFile(msg, "result")
  }

  private def printOnFile(msg: String, fileName: String): Unit = {
    val writer = new FileWriter(new File(s"$path/$fileName.csv"), true)
    writer.write(s"$msg\n")
    writer.close()
  }

}
