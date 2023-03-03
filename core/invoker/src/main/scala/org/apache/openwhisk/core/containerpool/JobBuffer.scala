package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.common.Logging

import scala.collection.SortedSet
import scala.collection.immutable.ListMap

class JobBuffer(private val content: ListMap[Option[ContainerId], SortedSet[Run]])(implicit val logging: Logging) {
  def size: Int = content.values.map(_.size).sum
  def length: Int = size

  def isEmpty: Boolean = size == 0
  def nonEmpty: Boolean = !isEmpty

  def enqueue(job: Run): JobBuffer = {
    val containerId: Option[ContainerId] = JobController.of(job).containerId
    new JobBuffer(content.updated(containerId, content.get(containerId) match {
      case Some(jobs) => jobs + job
      case None       => SortedSet[Run](job)(Ordering.by(_.offset))
    }))
  }

  def dequeueOption: Option[(Run, JobBuffer)] = if (isEmpty) None else Some(dequeue)

  def dequeue: (Run, JobBuffer) = {
    if (isEmpty) throw new NoSuchElementException("dequeue on empty buffer")

    val entry = content.head

    (entry._2.head, new JobBuffer(content.head._2 match {
      case jobs: Set[Run] if jobs.size > 1 => content.updated(entry._1, entry._2.tail)
      case _                               => content - entry._1
    }).reorder)
  }

  def reorder: JobBuffer = if (isEmpty) JobBuffer.empty else new JobBuffer(content.tail + content.head)

  def map[B](f: Run => B): List[B] = {
    var out = List.empty[B]
    for (jobs <- content.values)
      out = out ++ jobs.map(f)

    out
  }

  override def toString: String = {
    var out = List.empty[String]
    for (entry <- content)
      out = out :+ entry.toString()

    out.mkString("\n")
  }
}

object JobBuffer {
  def empty(implicit logging: Logging): JobBuffer = new JobBuffer(ListMap.empty)
}
