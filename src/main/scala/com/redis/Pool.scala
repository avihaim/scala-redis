package com.redis

import nf.fr.eraasoft.pool.impl.AbstractPool
import nf.fr.eraasoft.pool.{PoolableObject, PoolSettings}
import org.apache.commons.pool._
import org.apache.commons.pool.impl._
import com.redis.cluster.ClusterNode

class RedisClientFactory(val host: String, val port: Int, val database: Int = 0, val secret: Option[Any] = None, val timeout : Int = 0)
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

class RedisClientPool(val host: String, val port: Int, val maxIdle: Int = 8, val database: Int = 0, val secret: Option[Any] = None, val timeout : Int = 0)
                     (implicit optionPool : Option[ObjectPool[RedisClient]] = None) {

  val pool = optionPool.getOrElse(new StackObjectPool(new RedisClientFactory(host, port, database, secret, timeout), maxIdle))
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
 * @param node must be unique
 */
class IdentifiableRedisClientPool(val node: ClusterNode)
                                 (implicit optionPool : Option[ObjectPool[RedisClient]] = None)
  extends RedisClientPool (node.host, node.port, node.maxIdle, node.database, node.secret,node.timeout)(optionPool){
  override def toString = node.nodename
}