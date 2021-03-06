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

package org.apache.spark

import akka.actor.Actor
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler.TaskScheduler
import org.apache.spark.util.ActorLogReceive

/**
  * A heartbeat from executors to the driver. This is a shared message used by several internal
  * components to convey liveness or execution information for in-progress tasks.
  * <br>executor发送给driverActor的心跳信息
  */
private[spark] case class Heartbeat(
                                     executorId: String,
                                     taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
                                     blockManagerId: BlockManagerId)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
  * Lives in the driver to receive heartbeats from executors..
  * 存活在Driver中，用来接收executor的心跳
  *
  */
private[spark] class HeartbeatReceiver(scheduler: TaskScheduler)
  extends Actor with ActorLogReceive with Logging {

  /**
    * 处理Executor接收到的心跳
    */
  override def receiveWithLogging = {
    case Heartbeat(executorId, taskMetrics, blockManagerId) =>
      //其中的scheduler是TaskScheduler类型
      val response = HeartbeatResponse(!scheduler.executorHeartbeatReceived(executorId, taskMetrics, blockManagerId))
      sender ! response
  }
}
