package reactivemongo.util

import scala.collection.mutable.ArrayOps

import scala.util.hashing.MurmurHash3

import scala.reflect.ClassTag

/**
 * Note: This class is not thread-safe!
 */
final class SimpleRing[T: ClassTag](val capacity: Int) {
  private var head = 0
  private var tail = 0
  private var nextIndex = 0
  private var full = false
  private var empty = true
  private val queue = Array.ofDim[T](capacity)

  def enqueue(elem: T): Int = {
    queue(tail) = elem

    if (!full) {
      nextIndex += 1
      full = nextIndex == capacity
      empty = false
    } else {
      head = (head + 1) % capacity
    }

    tail = (head + nextIndex) % capacity

    nextIndex
  }

  def toArray(): Array[T] = {
    if (empty) {
      Array.empty[T]
    } else {
      val sz = nextIndex
      val readCopy = Array.ofDim[T](sz)
      val h = head

      if (sz < capacity) {
        Array.copy(queue, h, readCopy, 0, sz)
      } else {
        val s1 = capacity - h

        Array.copy(queue, h, readCopy, 0, s1)
        Array.copy(queue, 0, readCopy, s1, h)
      }

      readCopy
    }
  }

  def dequeue(): Option[T] = {
    if (empty) {
      Option.empty[T]
    } else {
      val elem = queue(head)

      nextIndex -= 1
      head = (head + 1) % capacity
      full = nextIndex == capacity

      Option(elem)
    }
  }

  private def internalState() = (capacity, head, tail, nextIndex)

  override def equals(that: Any): Boolean = that match {
    case other: SimpleRing[_] =>
      internalState() == other.internalState() &&
        (queue: ArrayOps[T]).equals(other.queue)

    case _ =>
      false
  }

  override def hashCode: Int = {
    val nh = MurmurHash3.mix(MurmurHash3.productSeed, internalState().##)

    MurmurHash3.mixLast(nh, MurmurHash3.arrayHash(queue))
  }
}
