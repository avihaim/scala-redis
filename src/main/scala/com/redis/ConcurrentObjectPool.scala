package com.redis

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.pool.PoolableObjectFactory

/**
 * User: avih
 * Date: 12/30/2014
 * Time: 11:32 AM
 */
class ConcurrentObjectPool[T](factory: PoolableObjectFactory[T],
                              maxIdle : Int) extends org.apache.commons.pool.ObjectPool[T] {
  val queue : util.Queue[T]= new ConcurrentLinkedQueue[T]
  val totalSize: AtomicInteger = new AtomicInteger(0)

  override def borrowObject(): T = {
    var t = queue.poll

    if (t == null) {
      t = factory.makeObject()
      totalSize.incrementAndGet
    }
    factory.activateObject(t)
    t
  }

  override def getNumActive: Int = {
    totalSize.get - queue.size
  }

  override def clear(): Unit = {
    while (queue.size > 0) {
      val t: T = queue.poll
      invalidateObject(t)
    }

    totalSize.set(0)
  }

  override def invalidateObject(p1: T): Unit = {
    factory.destroyObject(p1)
    totalSize.decrementAndGet
  }

  override def setFactory(p1: PoolableObjectFactory[T]): Unit = {}

  override def close(): Unit = {
    clear()
  }

  override def getNumIdle: Int = {
    maxIdle
  }

  override def addObject(): Unit = {

  }

  override def returnObject(p1: T): Unit = {
    if (p1 == null) return

    if ( factory.validateObject(p1)) {
      factory.passivateObject(p1)
      queue.add(p1)
    }

  }
}