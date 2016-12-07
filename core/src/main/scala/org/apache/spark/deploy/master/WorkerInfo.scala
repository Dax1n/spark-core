/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.master

import scala.collection.mutable

import akka.actor.ActorRef

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
  *
  * @param id
  * @param host
  * @param port
  * @param cores
  * @param memory
  * @param actor：ActorRef  worker的actor引用
  * @param webUiPort
  * @param publicAddress
  */
private[spark] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val cores: Int,
    val memory: Int,
    val actor: ActorRef,
    val webUiPort: Int,
    val publicAddress: String)
  extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  /**
    *  var executors: mutable.HashMap[String, ExecutorDesc] = _  <br>
    *     executorId 和ExecutorDesc的映射
    */
  @transient var executors: mutable.HashMap[String, ExecutorDesc] = _ // executorId => info
  @transient var drivers: mutable.HashMap[String, DriverInfo] = _ // driverId => info
  @transient var state: WorkerState.Value = _
  @transient var coresUsed: Int = _
  @transient var memoryUsed: Int = _

  @transient var lastHeartbeat: Long = _

  init()

  /**
    * 这是一个方法，求剩余的CPU核心数
    * @return  剩余的CPU核心数
    */
  def coresFree: Int = cores - coresUsed

  /**
    * 这是一个方法，求剩余的内存数
    * @return  剩余的内存数
    */
  def memoryFree: Int = memory - memoryUsed

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    executors = new mutable.HashMap
    drivers = new mutable.HashMap
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  /**
    * 把当前的Executor（ExecutorDesc描述的executor）添加到当前worker的executor记录表中
    * <br>修改当前worker的使用资源<br>1： coresUsed += exec.cores <br>2：memoryUsed += exec.memory
    * @param exec
    */
  def addExecutor(exec: ExecutorDesc) {
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
  }

  /**
    *把当前的Executor（ExecutorDesc描述的executor）从当前worker的executor记录表中移除
    * <br>修改当前worker的使用资源<br>1： coresUsed -= exec.cores <br>2：memoryUsed -= exec.memory
    * @param exec
    */
  def removeExecutor(exec: ExecutorDesc) {
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
    }
  }

  /**
    *
    * @param app ：判断app在该worker节点上是否已经启动executor了？
    * @return
    */
  def hasExecutor(app: ApplicationInfo): Boolean = {
    executors.values.exists(_.application == app)
  }

  def addDriver(driver: DriverInfo) {
    drivers(driver.id) = driver
    memoryUsed += driver.desc.mem
    coresUsed += driver.desc.cores
  }

  def removeDriver(driver: DriverInfo) {
    drivers -= driver.id
    memoryUsed -= driver.desc.mem
    coresUsed -= driver.desc.cores
  }

  def webUiAddress : String = {
    "http://" + this.publicAddress + ":" + this.webUiPort
  }

  def setState(state: WorkerState.Value) = {
    this.state = state
  }
}
