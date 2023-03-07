package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.common.Logging

import scala.collection.SortedSet
import scala.collection.immutable.ListMap

class JobBuffer(private val content: ListMap[Option[ContainerId], SortedSet[Run]])(implicit val logging: Logging) {

  /**
   * Get the jobs related to the first containerId in the ListMap, then select the job with the lowest offset.
   * Remove the selected job from the buffer.
   *
   * @return the selected job and the updated buffer.
   */
  def dequeue: (Run, JobBuffer) = {
    if (isEmpty) throw new NoSuchElementException("dequeue on empty buffer")

    val entry = content.head

    (entry._2.head, new JobBuffer(entry._2 match {
      case jobs: Set[Run] if jobs.size > 1 => content.updated(entry._1, entry._2.tail)
      case _                               => content - entry._1
    }).reorder)
  }

  def dequeueOption: Option[(Run, JobBuffer)] = if (isEmpty) None else Some(dequeue)

  /**
   * Put the received job among the ones related to the same containerId.
   * N.B. Jobs are ordered by offset.
   *
   * @param job is the job to be enqueued.
   * @return the updated buffer.
   */
  def enqueue(job: Run): JobBuffer = {
    val containerId: Option[ContainerId] = JobController.of(job).containerId
    new JobBuffer(content.updated(containerId, content.get(containerId) match {
      case Some(jobs) => jobs + job
      case None       => SortedSet[Run](job)(Ordering.by(_.offset))
    }))
  }

  def isEmpty: Boolean = size == 0

  def isReorderingUseful: Boolean = size > 1 && content.size > 1

  def length: Int = size

  def map[B](f: Run => B): List[B] = {
    var out = List.empty[B]
    for (jobs <- content.values)
      out = out ++ jobs.map(f)

    out
  }

  def nonEmpty: Boolean = !isEmpty

  /**
   * Change the order of the ListMap by moving the current head after the tail.
   * This is useful when a container is busy and we want to check there is work for other containers.
   *
   * @return the updated buffer.
   */
  def reorder: JobBuffer = if (isEmpty) JobBuffer.empty else new JobBuffer(content.tail + content.head)

  def size: Int = content.values.map(_.size).sum

  override def toString: String = {
    var out = List.empty[String]
    for (entry <- content)
      out = out :+ s"${entry._1.map {
        case c: ContainerId => c.asString takeRight 5
        case _              => ""
      }} -> [${entry._2.map(_.offset).mkString(", ")}]"

    s"{${out.mkString(", ")}}"
  }
}

object JobBuffer {
  def empty(implicit logging: Logging): JobBuffer = new JobBuffer(ListMap.empty)
}
