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
                              maxIdle : Int, val minSize : Int = 4) extends org.apache.commons.pool.ObjectPool[T] {
  val queue : util.Queue[T]= new ConcurrentLinkedQueue[T]
  val totalSize: AtomicInteger = new AtomicInteger(0)
  val idleSize: AtomicInteger = new AtomicInteger(0)

  if(minSize > 0) {
    for (i <- 0 to minSize) {
      addObject()
    }
  }

  override def borrowObject(): T = {
    var t = queue.poll

    if (t == null) {
      t = factory.makeObject()
      totalSize.incrementAndGet
    } else {
      idleSize.decrementAndGet()
    }
    factory.activateObject(t)
    t
  }

  override def getNumActive: Int = {
    totalSize.get - queue.size
  }

  override def clear(): Unit = {
    var t: T = queue.poll
    while (t != null) {
      invalidateObject(t)
      t = queue.poll
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
    idleSize.get()
  }

  override def addObject(): Unit = {
    queue.add(factory.makeObject())
  }

  override def returnObject(p1: T): Unit = {
    if (p1 == null) return

    if (factory.validateObject(p1) ) {
      factory.passivateObject(p1)

      if(idleSize.incrementAndGet() <= maxIdle) {
        queue.add(p1)
      } else {
        idleSize.decrementAndGet()
        invalidateObject(p1)
      }
    } else {
      invalidateObject(p1)
    }

  }
}