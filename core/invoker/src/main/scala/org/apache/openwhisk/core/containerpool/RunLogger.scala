package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.core.entity.ActivationResponse

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

object RunLogger {
  private val path: String = {
    val path = s"./logs"
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
    val msg = s"ARRIVAL - ${job.offset} " +
      s"${getActorLabel(job)} ${job.msg.getContentField("message")} " +
      s"${System.currentTimeMillis()}"

    printOnFile(msg)
  }

  def buffered(job: Run): Unit = {
    val msg = s"BUFFER - ${job.offset} " +
      s"${getActorLabel(job)} ${job.msg.getContentField("message")}" +
      s"${System.currentTimeMillis()}"

    printOnFile(msg)
  }

  def execution(job: Run): Unit = {
    val msg = s"EXECUTION - ${job.offset} " +
      s"${getActorLabel(job)} ${job.msg.getContentField("message")} " +
      s"${System.currentTimeMillis()} "

    printOnFile(msg)
  }

  def result(job: Run, interval: Interval, response: ActivationResponse): Unit = {
    val msg = s"RESULT - ${job.offset} " +
      s"${getActorLabel(job)} ${job.msg.getContentField("message")} " +
      s"${System.currentTimeMillis()} " +
      s"${interval.duration.length}\n" +
      s"${response.result.get.prettyPrint}\n"

    printOnFile(msg)
  }

  private def printOnFile(msg: String): Unit = {
    val writer = new FileWriter(new File(s"$path/$formattedDateTime.csv"), true)
    writer.write(s"$msg\n")
    writer.close()
  }

}
