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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.util.CallSite

/**
  * Tracks information about an active job in the DAGScheduler.
  *
  * 记录该 DAGScheduler中 active job的信息<br>指标有：<br>
  *<br> 1：numPartitions 分区数 <br>2：finished 分区完成情况<br>3：numFinished完成个数
  *
  */
private[spark] class ActiveJob(
                                val jobId: Int,
                                val finalStage: Stage,
                                val func: (TaskContext, Iterator[_]) => _,
                                val partitions: Array[Int],
                                val callSite: CallSite,
                                val listener: JobListener,
                                val properties: Properties) {

  val numPartitions = partitions.length
  /**
    * Array.fill[Boolean](numPartitions)(false)是初始化一个numPartitions大小的数组，其中每一个元素的值为false<br>
    *   <br>
    */
  val finished = Array.fill[Boolean](numPartitions)(false)

  var numFinished = 0


}
