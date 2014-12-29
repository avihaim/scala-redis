package com.redis

import nf.fr.eraasoft.pool.impl.AbstractPool
import nf.fr.eraasoft.pool.{PoolableObject, PoolSettings}
import org.apache.commons.pool._
import org.apache.commons.pool.impl._
import com.redis.cluster.ClusterNode

private [redis] class RedisClientFactory(val host: String, val port: Int, val database: Int = 0, val secret: Option[Any] = None, val timeout : Int = 0)
  extends PoolableObjectFactory[RedisClient] {

  // when we make an object it's already connected
  def makeObject = {
    new RedisClient(host, port, database, secret, timeout)
  }

  // quit & disconnect
  def destroyObject(rc: RedisClient): Unit = {
    rc.quit // need to quit for closing the connection
    rc.disconnect // need to disconnect for releasing sockets
  }

  // noop: we want to have it connected
  def passivateObject(rc: RedisClient): Unit = {}
  def validateObject(rc: RedisClient) = rc.connected == true

  // noop: it should be connected already                                                     org.apache.commons.pool.ObjectPool<T>
  def activateObject(rc: RedisClient): Unit = {}
}

class RedisClientPool(val host: String, val port: Int, val maxIdle: Int = 8, val database: Int = 0, val secret: Option[Any] = None, val timeout : Int = 0) {

  val pool : org.apache.commons.pool.ObjectPool[RedisClient] = new ConcurrentObjectPool(new RedisClientFactory(host, port, database, secret, timeout), maxIdle)

//  val pool = new StackObjectPool(new RedisClientFactory(host, port, database, secret, timeout), maxIdle)
  override def toString = host + ":" + String.valueOf(port)

  def withClient[T](body: RedisClient => T) = {
    val client = pool.borrowObject
    try {
      body(client)
    } finally {
      pool.returnObject(client)
    }
  }

  // close pool & free resources
  def close = pool.close
}

/**
 *
 * @param poolname must be unique
 */
class IdentifiableRedisClientPool(val node: ClusterNode)
  extends RedisClientPool (node.host, node.port, node.maxIdle, node.database, node.secret,node.timeout){
  override def toString = node.nodename
}


class ConcurrentObjectPool[T](factory: PoolableObjectFactory[T],
                              maxIdle : Int) extends org.apache.commons.pool.ObjectPool[T] {

  val poolSettings = new PoolSettings[T](new PoolableObject[T]() {
    override def make(): T = factory.makeObject()

    override def passivate(p1: T): Unit = factory.passivateObject(p1)

    override def destroy(p1: T): Unit = factory.destroyObject(p1)

    override def activate(p1: T): Unit = factory.activateObject(p1)

    override def validate(p1: T): Boolean = factory.validateObject(p1)
  })

  poolSettings.maxIdle(maxIdle)

  val pool : AbstractPool[T] = poolSettings.pool().asInstanceOf

  override def borrowObject(): T = {
    pool.getObj
  }

  override def getNumActive: Int = {
    pool.actives()
  }

  override def clear(): Unit = {
    pool.clear()
  }

  override def invalidateObject(p1: T): Unit = {

  }

  @deprecated
  override def setFactory(p1: PoolableObjectFactory[T]): Unit = {}

  override def close(): Unit = {

  }

  override def getNumIdle: Int = {
    maxIdle
  }

  override def addObject(): Unit = {

  }

  override def returnObject(p1: T): Unit = {
    pool.returnObj(p1)
  }
}