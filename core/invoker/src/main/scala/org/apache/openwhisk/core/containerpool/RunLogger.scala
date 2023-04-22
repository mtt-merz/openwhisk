package org.apache.openwhisk.core.containerpool

import spray.json.JsValue

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

object RunLogger {
  private var path: Option[String] = Option.empty

  def log(msg: String, data: Option[JsValue] = Option.empty): Unit = {
    if (path.isEmpty)
      path = Option(
        s"/home/m/Workspaces/openwhisk/" +
          s"logs/$formattedDateTime.txt")

    val m = s"[$formattedDateTime] $msg" +
      (if (data.isDefined) s"\n${data.get.prettyPrint}\n")

    val writer = new FileWriter(new File(path.get), true)
    writer.write(s"$m\n")
    writer.close()
  }

//  private def formattedDate: String = {
//    val formatter = new SimpleDateFormat("YYYY-MM-DD")
//    formatter.format(Calendar.getInstance().getTime)
//  }

  private def formattedDateTime: String = {
    val formatter = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss.sss")
    formatter.format(Calendar.getInstance().getTime)
  }
}
