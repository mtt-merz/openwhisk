package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.common.Logging

import scala.collection.SortedSet
import scala.collection.immutable.ListMap

class RunBuffer(private val content: ListMap[Option[ContainerId], SortedSet[Run]])(implicit val logging: Logging) {

  /**
   * Get the runs related to the first containerId in the ListMap, then select the run with the lowest offset.
   * Remove the selected run from the buffer.
   *
   * @return the selected run and the updated buffer.
   */
  def dequeue: (Run, RunBuffer) = {
    if (isEmpty) throw new NoSuchElementException("dequeue on empty buffer")

    val entry = content.head

    (entry._2.head, new RunBuffer(entry._2 match {
      case runs: Set[Run] if runs.size > 1 => content.updated(entry._1, entry._2.tail)
      case _                               => content - entry._1
    }).reorder)
  }

  def dequeueOption: Option[(Run, RunBuffer)] = if (isEmpty) None else Some(dequeue)

  /**
   * Put the received run among the ones related to the same containerId.
   * N.B. Runs are ordered by offset.
   *
   * @param run is the run to be enqueued.
   * @return the updated buffer.
   */
  def enqueue(run: Run): RunBuffer = {
    logging.info(this, s"Enqueue request #${run.offset}\n")(run.msg.transid)

    val containerId: Option[ContainerId] = RunController.of(run).containerId
    new RunBuffer(content.updated(containerId, content.get(containerId) match {
      case Some(jobs) => jobs + run
      case None       => SortedSet[Run](run)(Ordering.by(_.offset))
    }))
  }

  def isEmpty: Boolean = size == 0

  def isReorderingUseful: Boolean = size > 1 && content.size > 1

  def map[B](f: Run => B): List[B] = {
    var out = List.empty[B]
    for (runs <- content.values)
      out = out ++ runs.map(f)

    out
  }

  def nonEmpty: Boolean = !isEmpty

  /**
   * Change the order of the ListMap by moving the current head after the tail.
   * This is useful when a container is busy and we want to check there is work for other containers.
   *
   * @return the updated buffer.
   */
  def reorder: RunBuffer = if (isReorderingUseful) new RunBuffer(content.tail + content.head) else this

  def size: Int = content.values.map(_.size).sum

  override def toString: String = {
    var out = List.empty[String]
    for (entry <- content)
      out = out :+ s"${entry._1.map {
        case c: ContainerId => "..." + c.asString takeRight 5
        case _              => ""
      }} -> [${entry._2.map(_.offset).mkString(", ")}]"

    s"{${out.mkString(", ")}}"
  }
}

object RunBuffer {
  def empty(implicit logging: Logging): RunBuffer = new RunBuffer(ListMap.empty)
}
