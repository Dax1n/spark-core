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

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.{TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map, Stack}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.pattern.ask
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._
import org.apache.spark.util._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat

/**
  * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
  * stages for each job, keeps track of（记录，跟踪） which RDDs and stage outputs are materialized, and finds a
  * minimal（最低的，最小的） schedule to run the job.
  * <br>
  * It then submits stages as TaskSets to an underlying TaskScheduler implementation that runs them on the cluster.
  * <br>然后DAGScheduler将stages作为TaskSets提交 给实现TaskScheduler接口的运行在集群上的 XXXTaskScheduler（例如TaskSchedulerImpl）中
  * <br>
  * <br>
  * <br>
  * <br> 这是一个高层次的调度层，面向stage调度的。DAGScheduler为每一个job计算stages的DAG
  * <br>
  * <br>
  *
  * In addition to coming up with a DAG of stages, this class also determines the preferred
  * locations to run each task on, based on the current cache status, and passes these to the
  * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
  * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
  * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
  * a small number of times before cancelling the whole stage.
  *
  * 除了提出一个DAG of stages，DAGScheduler还能根据位置选择行的在其上执行task，没有翻译完
  *
  * Here's a checklist to use when making or reviewing changes to this class:
  *
  *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
  * include the new structure. This will help to catch memory leaks.
  *
  *
  * <br><br><br><br>DAGScheduler负责拆分成Stage的具有依赖关系（包含RDD的依赖关系）的多批任务，然后提交给TaskScheduler进行具体处理。
  * <br><br>
  *  [[在DAGScheduler创建主构造器最后一行完成一件大事]]<br> 就是启动DAGScheduler的时间循环处理器（DAGSchedulerEventProcessLoop的一个实例），
  * <br>DAGScheduler的eventProcessLoop中有一个继承自父类的Thread一直循环处理消息，在该线程中获取到消息之后发送给
  * <br> DAGSchedulerEventProcessLoop的OnRecieve方法模式匹配处理
  *
  */

// TODO [[在DAGScheduler创建主构造器最后一行完成一件大事]]
private[spark] class DAGScheduler(
                                   private[scheduler] val sc: SparkContext,
                                   private[scheduler] val taskScheduler: TaskScheduler,
                                   listenerBus: LiveListenerBus,
                                   mapOutputTracker: MapOutputTrackerMaster,
                                   blockManagerMaster: BlockManagerMaster,
                                   env: SparkEnv, clock: Clock = new SystemClock()) extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[scheduler] val nextJobId = new AtomicInteger(0)

  private[scheduler] def numTotalJobs: Int = nextJobId.get()

  private val nextStageId = new AtomicInteger(0)

  /**
    * private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]<br>
    * jobId ->StageIds的映射
    *
    */
  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]


  /**
    * HashMap映射 ：  shuffleId->Stage
    */
  private[scheduler] val shuffleToMapStage = new HashMap[Int, Stage]

  /**
    * HashMap数据结构：key为jobId value为ActiveJob<br>
    * 记录当前ActiveJob的容器
    *
    */
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  /**
    * activeJobs
    */
  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
    * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
    * and its values are arrays indexed by partition numbers. Each array value is the set of
    * locations where that RDD partition is cached.
    * <br>记录的每一个RDD 分区被缓存的位置<br>key为RDD的id Value为是一个数组，数组的index为partition的编号<br>
    * 每一个数组的元素值是RDD partition 缓存的位置
    *
    * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).<br>
    * 访问这个map必须使用synchronize关键字
    */
  private val cacheLocs = new HashMap[Int, Array[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private[scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  /**
    * A closure serializer that we reuse.
    * <br> This is only safe because DAGScheduler runs in a single thread.
    */
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()


  /** If enabled, we may run certain actions like take() and first() locally.
    * <br>例如我们只在RDD做一个take() 或者 first() 这样很简单的处理话，我们可以使用本地运行
    * <br>localExecutionEnabled的声明：<br>
    * private val localExecutionEnabled = sc.getConf.getBoolean("spark.localExecution.enabled", false)
    */
  private val localExecutionEnabled = sc.getConf.getBoolean("spark.localExecution.enabled", false)

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  private val messageScheduler =
    Executors.newScheduledThreadPool(1, Utils.namedThreadFactory("dag-scheduler-message"))

  /**
    * eventProcessLoop是DAGScheduler的一个DAGSchedulerEventProcessLoop实例
    */
  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  // Called by TaskScheduler to report task's starting.
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  // Called to report that a task has completed and results are being fetched remotely.
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  // Called by TaskScheduler to report task completions or failures.
  def taskEnded(
                 task: Task[_],
                 reason: TaskEndReason,
                 result: Any,
                 accumUpdates: Map[Long, Any],
                 taskInfo: TaskInfo,
                 taskMetrics: TaskMetrics) {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo, taskMetrics))
  }

  /**
    * Update metrics for in-progress tasks and let the master know that the BlockManager is still
    * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
    * indicating that the block manager should re-register.
    */
  def executorHeartbeatReceived(
                                 execId: String,
                                 taskMetrics: Array[(Long, Int, Int, TaskMetrics)], // (taskId, stageId, stateAttempt, metrics)
                                 blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, taskMetrics))
    implicit val timeout = Timeout(600 seconds)

    Await.result(
      blockManagerMaster.driverActor ? BlockManagerHeartbeat(blockManagerId),
      timeout.duration).asInstanceOf[Boolean]
  }

  // Called by TaskScheduler when an executor fails.
  def executorLost(execId: String) {
    eventProcessLoop.post(ExecutorLost(execId))
  }

  // Called by TaskScheduler when a host is added
  def executorAdded(execId: String, host: String) {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  // Called by TaskScheduler to cancel an entire TaskSet due to either repeated failures or
  // cancellation of the job itself.
  def taskSetFailed(taskSet: TaskSet, reason: String) {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason))
  }

  private def getCacheLocs(rdd: RDD[_]): Array[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      val blockIds = rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
      val locs = BlockManager.blockIdsToBlockManagers(blockIds, env, blockManagerMaster)
      cacheLocs(rdd.id) = blockIds.map { id =>
        locs.getOrElse(id, Nil).map(bm => TaskLocation(bm.host, bm.executorId))
      }
    }
    cacheLocs(rdd.id)
  }

  /**
    * 清除每一个RDD分区被缓存的位置信息<br>
    */
  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
    * Get or create a shuffle map stage for the given shuffle dependency's map side.
    * The jobId value passed in will be used if the stage doesn't already exist with
    * a lower jobId (jobId always increases across jobs.)<br><br>
    *
    * <br><br>根据ShuffleDependency和jobId获取该stage
    */
  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): Stage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None => // TODO 第一次shuffleToMapStage中没有，所以匹配None
        // We are going to register ancestor shuffle dependencies
        // 我们打算注册祖先的shuffle dependencies
        registerShuffleDependencies(shuffleDep, jobId)
        // Then register current shuffleDep
        val stage =
        newOrUsedStage(
          shuffleDep.rdd, shuffleDep.rdd.partitions.size, shuffleDep, jobId,
          shuffleDep.rdd.creationSite)
        shuffleToMapStage(shuffleDep.shuffleId) = stage

        stage
    }
  } //getShuffleMapStage结束

  /**
    * Create a Stage -- either directly for use as a result stage, or as part of the (re)-creation<br>
    * of a shuffle map stage in newOrUsedStage.  The stage will be associated with the provided<br>
    * jobId. Production of shuffle map stages should always use newOrUsedStage, not newStage directly.<br>
    * <br><br>
    * 译文：
    *
    * <br>用于创建stage
    *
    */
  //TODO 用于创建stage
  private def newStage(
                        rdd: RDD[_],
                        numTasks: Int,
                        shuffleDep: Option[ShuffleDependency[_, _, _]],
                        jobId: Int,
                        callSite: CallSite): Stage = {

    //TODO 获取rdd（finalRDD）的父Stage
    val parentStages = getParentStages(rdd, jobId)

    val id = nextStageId.getAndIncrement()

    val stage = new Stage(id, rdd, numTasks, shuffleDep, parentStages, jobId, callSite)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
    * Create a shuffle map Stage for the given RDD.  The stage will also be associated with the
    * provided jobId.  If a stage for the shuffleId existed previously so that the shuffleId is
    * present in the MapOutputTracker, then the number and location of available outputs are
    * recovered from the MapOutputTracker<br><br>
    *
    *
    */
  private def newOrUsedStage(
                              rdd: RDD[_],
                              numTasks: Int,
                              shuffleDep: ShuffleDependency[_, _, _],
                              jobId: Int,
                              callSite: CallSite): Stage = {


    //TODO 递归调用，因为最起初前面第一次就是通过调用newStage函数进来的，此处又调用newStage
    val stage = newStage(rdd, numTasks, Some(shuffleDep), jobId, callSite)

    // MapOutputTrackerMaster 是驱动的一个map输出跟踪器
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // TODO 判断该Shuffle在Driver端是否有map输出跟踪器
      // TODO 有map输出跟踪器
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.size) {
        stage.outputLocs(i) = Option(locs(i)).toList // locs(i) will be null if missing
      }

      stage.numAvailableOutputs = locs.count(_ != null)

    } else {
      // TODO 没有map输出跟踪器
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      // TODO 注册map输出跟踪器
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.size)
    }
    stage
  }

  /**
    * Get or create the list of parent stages for a given RDD. The stages will be assigned the
    * provided jobId if they haven't already been created with a lower jobId.
    * <br><br>
    * 根据传入的RDD获取或者创建一个父Stage的list。stage会被指定一个jobID
    *
    */
  //TODO 用来获取父Stage
  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    /**
      * 用于存储stage的集合
      */
    val parents = new HashSet[Stage]
    /**
      * 记录RDD是否访问过
      */
    val visited = new HashSet[RDD[_]] //RDD[_]中的下划线表示RDD中的元素任意类型
    /**
      * We are manually maintaining a stack here to prevent StackOverflowError caused by recursively visiting
      * <br><br>我们会认为维护一个栈避免递归访问时候出 “栈溢出错误”
      */
    val waitingForVisit = new Stack[RDD[_]]
    /**
      * 定义在getParentStages方法中的方法！！
      */
    //TODO 定义在getParentStages方法中的方法！！
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        //visited(r)是调用HashSet的apply方法，该方法作用就是判断该HashSet中是否含有r元素
        // 没有被访问过的rdd，再此处访问并记录到已访问你的集合中
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          //TODO 对rdd的依赖进行遍历之后模式匹配判断依赖类型！！！
          dep match {
            case shufDep: ShuffleDependency[_, _, _] => //按照类型进行匹配
              //TODO 如果是Shuffle依赖，调用getShuffleMapStage方法求该shuffle依赖的stage，然后记录到依赖集合中
              parents += getShuffleMapStage(shufDep, jobId)
            case _ =>
              //不是宽依赖的话，将依赖前面的rdd存储到栈中
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    //将最后的rdd存储到栈中
    waitingForVisit.push(rdd)
    //TODO 重点！！！:
    while (!waitingForVisit.isEmpty) {
      //栈不空递归遍历
      visit(waitingForVisit.pop()) //调用visit
    }
    parents.toList
  } //getParentStages定义结束


  // Find ancestor missing shuffle dependencies and register into shuffleToMapStage
  // TODO 找到祖先的shuffle dependencies 注册到shuffleToMapStage
  private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], jobId: Int) = {

    // TODOshuffleDep的父ShuffleDependencies
    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)


    // TODO parentsWithNoMapStage不等于空的话，证明前面还有ShuffleDependency
    while (!parentsWithNoMapStage.isEmpty) {
      // TODO 进来继续查找前面的ShuffleDependency
      val currentShufDep = parentsWithNoMapStage.pop()
      // TODO 继续查找前面的ShuffleDependency
      val stage = newOrUsedStage(
        currentShufDep.rdd, currentShufDep.rdd.partitions.size, currentShufDep, jobId,
        currentShufDep.rdd.creationSite)

      shuffleToMapStage(currentShufDep.shuffleId) = stage
    }

  }


  /**
    * Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet
    * <br> 找到没有注册在shuffleToMapStage中的祖先的shuffle dependencies
    * <br>也是递归将父shuffle dependencies注册到shuffleToMapStage中
    *
    * @param rdd
    * @return
    */
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {

    // TODO 此处逻辑多次定义，微小区别。别混淆
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }

              waitingForVisit.push(shufDep.rdd)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }

    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        if (getCacheLocs(rdd).contains(Nil)) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.jobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
    * Registers the given jobId among the jobs that need the given stage and
    * all of that stage's ancestors.
    */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage) {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter {
          !_.jobIds.contains(jobId)
        }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
    * Removes state for job and any stages that are not needed by any other job.  Does not
    * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
    *
    * @param job The job whose state to cleanup.
    */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob) {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
                .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) {
              // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage.resultOfJob = None
  }

  /**
    * Submit a job to the job scheduler and get a JobWaiter object back. The JobWaiter object
    * can be used to block until the the job finishes executing or can be used to cancel the job.
    * <br>
    * 提交一个作业给作业调度器，然后返回一个JobWaiter对象。<br>
    * 返回JobWaiter对象这个过程是阻塞的，直到作业完成计算或者作业取消计算返回。
    */
  //TODO 真正触发作业运行的
  def submitJob[T, U](
                       rdd: RDD[T],
                       func: (TaskContext, Iterator[T]) => U,
                       partitions: Seq[Int],
                       callSite: CallSite,
                       allowLocal: Boolean,
                       resultHandler: (Int, U) => Unit,
                       properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    //TODO 译文: 确保启动的每一个Task都有对应的partition存在
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)

    // TODO 把提交的作业放到 DAGSchedulerEventProcessLoop中,
    eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions.toArray, allowLocal, callSite, waiter, properties))

    waiter
  }

  /**
    * Run an action job on the given RDD and pass all the results to the resultHandler function as
    * they arrive.
    *
    * @param rdd           target RDD to run tasks on
    * @param func          a function to run on each partition of the RDD
    * @param partitions    set of partitions to run on; some jobs may not want to compute on all
    *                      partitions of the target RDD, e.g. for operations like first()
    * @param callSite      where in the user program this job was called
    * @param allowLocal
    * @param resultHandler callback to pass each result to
    * @param properties    scheduler properties to attach to this job, e.g. fair scheduler pool name
    * @throws Exception when the job fails
    *                   <br><br>正真触发一个作业运行的
    *
    */
  //TODO DAGScheduler的 runJob方法，用来切分Stage的
  def runJob[T, U: ClassTag](
                              rdd: RDD[T],
                              func: (TaskContext, Iterator[T]) => U,
                              partitions: Seq[Int],
                              callSite: CallSite,
                              allowLocal: Boolean,
                              resultHandler: (Int, U) => Unit,
                              properties: Properties): Unit = {

    val start = System.nanoTime
    //TODO 真正触发作业运行的
    val waiter = submitJob(rdd, func, partitions, callSite, allowLocal, resultHandler, properties)

    //作业执行完毕，模式匹配完成的状态，并记录日志
    waiter.awaitResult() match {
      case JobSucceeded => {
        logInfo("Job %d finished: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      }
      case JobFailed(exception: Exception) =>
        logInfo("Job %d failed: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        throw exception
    }
  } // runJob方法定义结束


  def runApproximateJob[T, U, R](
                                  rdd: RDD[T],
                                  func: (TaskContext, Iterator[T]) => U,
                                  evaluator: ApproximateEvaluator[U, R],
                                  callSite: CallSite,
                                  timeout: Long,
                                  properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.size).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, allowLocal = false, callSite, listener, properties))
    listener.awaitResult() // Will throw an exception if the job fails
  }

  /**
    * Cancel a job that is running or waiting in the queue.
    */
  def cancelJob(jobId: Int) {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  def cancelJobGroup(groupId: String) {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
    * Cancel all jobs that are running or waiting in the queue.
    */
  def cancelAllJobs() {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.jobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
    submitWaitingStages()
  }

  /**
    * Cancel all jobs associated with a running or scheduled stage.
    */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
    * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
    * the last fetch failure.
    */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.jobId)) {
        submitStage(stage)
      }
    }
    submitWaitingStages()
  }

  /**
    * Check for waiting or failed stages which are now eligible for resubmission.
    * Ordinarily run on every iteration of the event loop.
    */
  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()
    for (stage <- waitingStagesCopy.sortBy(_.jobId)) {
      submitStage(stage)
    }
  }

  /**
    * Run a job on an RDD locally, assuming it has only a single partition and no dependencies.
    * We run the operation in a separate thread just in case it takes a bunch of time, so that we
    * don't block the DAGScheduler event loop or other concurrent jobs.<br>
    * 在RDD所在的位置本地运行一个job，它只有一个分区和无依赖，我们运行一个线程处理，<br>
    * 这么做的好处是不阻塞DAGScheduler事件循环处理和其他并发作业
    *
    */
  protected def runLocally(job: ActiveJob) {
    logInfo("Computing the requested partition locally")
    new Thread("Local computation of job " + job.jobId) {
      override def run() {
        runLocallyWithinThread(job)
      }
    }.start()
  }

  // Broken out for easier testing in DAGSchedulerSuite.
  protected def runLocallyWithinThread(job: ActiveJob) {
    var jobResult: JobResult = JobSucceeded
    try {
      val rdd = job.finalStage.rdd
      val split = rdd.partitions(job.partitions(0))
      val taskContext = new TaskContextImpl(job.finalStage.id, job.partitions(0), taskAttemptId = 0,
        attemptNumber = 0, runningLocally = true)
      TaskContextHelper.setTaskContext(taskContext)
      try {
        val result = job.func(taskContext, rdd.iterator(split, taskContext))
        job.listener.taskSucceeded(0, result)
      } finally {
        taskContext.markTaskCompleted()
        TaskContextHelper.unset()
      }
    } catch {
      case e: Exception =>
        val exception = new SparkDriverExecutionException(e)
        jobResult = JobFailed(exception)
        job.listener.jobFailed(exception)
      case oom: OutOfMemoryError =>
        val exception = new SparkException("Local job aborted due to out of memory error", oom)
        jobResult = JobFailed(exception)
        job.listener.jobFailed(exception)
    } finally {
      val s = job.finalStage
      // clean up data structures that were populated for a local job,
      // but that won't get cleaned up via the normal paths through
      // completion events or stage abort
      stageIdToStage -= s.id
      jobIdToStageIds -= job.jobId
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), jobResult))
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {

    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted

    jobsThatUseStage.find(jobIdToActiveJob.contains)

  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter(activeJob =>
      Option(activeJob.properties).exists(_.get(SparkContext.SPARK_JOB_GROUP_ID) == groupId))
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
    submitWaitingStages()
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleTaskSetFailed(taskSet: TaskSet, reason: String) {
    stageIdToStage.get(taskSet.stageId).foreach {
      abortStage(_, reason)
    }
    submitWaitingStages()
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error = new SparkException("Job cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    submitWaitingStages()
  }


  /**
    *
    * 在DAGSchedulerEventProcessLoop中的消息队列中获取消息，模式匹配调用了handleJobSubmitted提交作业
    *
    * @param jobId
    * @param finalRDD
    * @param func
    * @param partitions
    * @param allowLocal 是否允许本地运行
    * @param callSite
    * @param listener
    * @param properties
    */
  //TODO  用来切分stage
  private[scheduler] def handleJobSubmitted(jobId: Int,
                                            finalRDD: RDD[_],
                                            func: (TaskContext, Iterator[_]) => _,
                                            partitions: Array[Int],
                                            allowLocal: Boolean,
                                            callSite: CallSite,
                                            listener: JobListener,
                                            properties: Properties) {
    var finalStage: Stage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      //译文：创建stage可能抛出异常，例如：job对的运行是基于HadoopRDD的，但是创建RDD的文件被删除就会抛异常
      //TODO finalStage是最后一个stage。
      finalStage = newStage(finalRDD, partitions.size, None, jobId, callSite)

    } catch {

      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    // TODO finalStage 不为NUll则提交
    if (finalStage != null) {
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)

      // TODO 清除每一个RDD分区缓存的位置，因为此时认为还没有提交。所以必须保证记录RDD分区缓存位置的容器cacheLocs为空的

      clearCacheLocs()
      logInfo("Got job %s (%s) with %d output partitions (allowLocal=%s)".format(
        job.jobId, callSite.shortForm, partitions.length, allowLocal))
      logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))



      /**
        * localExecutionEnabled  是否开启了本地执行（在rdd进行特别简单的一次操作的话，可以开启）
        *
        * allowLocal  是否允许本地执行
        *
        * finalStage.parents.isEmpty finalStage前面没有stage
        *
        * partitions.length  只有一个分区
        *
        */
      val shouldRunLocally = localExecutionEnabled && allowLocal && finalStage.parents.isEmpty && partitions.length == 1

      //作业提交时间
      val jobSubmissionTime = clock.getTimeMillis()

      if (shouldRunLocally) {
        // Compute very short actions like first() or take() with no parent stages locally.
        listenerBus.post(SparkListenerJobStart(job.jobId, jobSubmissionTime, Seq.empty, properties))
        //见方法注释
        runLocally(job)
      } else {

        //记录activeJobs
        jobIdToActiveJob(jobId) = job
        activeJobs += job


        finalStage.resultOfJob = Some(job)

        //获取该jobId的 stageIds
        val stageIds = jobIdToStageIds(jobId).toArray

        // TODO 把stage生成stageInfo
        val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))

        listenerBus.post(SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))

        // TODO 递归提交每一个stage
        submitStage(finalStage)
      }

    }
    submitWaitingStages()
  }

  /**
    * Submits stage, but first recursively submits any missing parents.<br>
    * 提交stage，递归提交每一个stage的parent stage
    *
    */
  private def submitStage(stage: Stage) {

    val jobId = activeJobForStage(stage)

    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      // TODO  waitingStages中不含有该stage 并且runningStages中不含有该stage 并且failedStages不含有该stage 则提交
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {

        val missing = getMissingParentStages(stage).sortBy(_.id)

        logDebug("missing: " + missing)

        //TODO 重要重要重要   ：这个递归提交的终止条件
        if (missing == Nil) {
          //TODO  missing中不存在stage了，则提交stage (此时：stage是第一个stage，该stage前面在没有stage了)
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {

            //TODO 调用递归实现递归提交
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id)
    }
  }


  /**
    *
    * Called when stage's parents are available and we can now do its task.
    *
    *
    *
    *
    */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingTasks.clear()

    /**
      * First figure out the indexes of partition ids to compute.<br>
      * 将要计算的分区ids
      */


    val partitionsToCompute: Seq[Int] = {
      //TODO 首先查找出将要计算的分区ids
      if (stage.isShuffleMap) {
        (0 until stage.numPartitions).filter(id => stage.outputLocs(id) == Nil)
      } else {
        val job = stage.resultOfJob.get
        (0 until job.numPartitions).filter(id => !job.finished(id))
      }
    }

    val properties = if (jobIdToActiveJob.contains(jobId)) {
      jobIdToActiveJob(stage.jobId).properties
    } else {
      // this stage will be assigned to "default" pool
      null
    }

    //添加到runningStages
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage.latestInfo = StageInfo.fromStage(stage, Some(partitionsToCompute.size))
    outputCommitCoordinator.stageStart(stage.id)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.

    //TODO 这个是广播变量！
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = //TODO
        if (stage.isShuffleMap) {
          //TODO  ShuffleMapTask
          closureSerializer.serialize((stage.rdd, stage.shuffleDep.get): AnyRef).array()
        } else {
          //TODO ResultTask结果
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
        }


      // 把任务广播出去
      taskBinary = sc.broadcast(taskBinaryBytes)


    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString)
        runningStages -= stage
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
        runningStages -= stage
        return
    }

    //TaskSet
    val tasks: Seq[Task[_]] = if (stage.isShuffleMap) {

      //TODO map 返回一组ShuffleMapTask
      partitionsToCompute.map { id =>

        val locs = getPreferredLocs(stage.rdd, id)
        //根据id获取分区
        val part = stage.rdd.partitions(id)

        //拉取上游数据
        new ShuffleMapTask(stage.id, taskBinary, part, locs)

      }
    } else {

      //TODO ResultTask
      val job = stage.resultOfJob.get
      partitionsToCompute.map { id =>
        val p: Int = job.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = getPreferredLocs(stage.rdd, p)

        //结果持久化或者打印，返回Driver等
        new ResultTask(stage.id, taskBinary, part, locs, id)
      }
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingTasks ++= tasks
      logDebug("New pending tasks: " + stage.pendingTasks)


      //TODO  传说中的TaskScheduler出现了，最终是TaskScheduler提交TaskSet
      //submitTasks实现：  org.apache.spark.scheduler.TaskSchedulerImpl.submitTasks
      taskScheduler.submitTasks(new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)
      logDebug("Stage " + stage + " is actually done; %b %d %d".format(
        stage.isAvailable, stage.numAvailableOutputs, stage.numPartitions))
    }
  }


  /** Merge updates from a task to our local accumulator values */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    if (event.accumUpdates != null) {
      try {
        Accumulators.add(event.accumUpdates)
        event.accumUpdates.foreach { case (id, partialValue) =>
          val acc = Accumulators.originals(id).asInstanceOf[Accumulable[Any, Any]]
          // To avoid UI cruft, ignore cases where value wasn't updated
          if (acc.name.isDefined && partialValue != acc.zero) {
            val name = acc.name.get
            val stringPartialValue = Accumulators.stringifyPartialValue(partialValue)
            val stringValue = Accumulators.stringifyValue(acc.value)
            stage.latestInfo.accumulables(id) = AccumulableInfo(id, name, stringValue)
            event.taskInfo.accumulables +=
              AccumulableInfo(id, name, Some(stringPartialValue), stringValue)
          }
        }
      } catch {
        // If we see an exception during accumulator update, just log the
        // error and move on.
        case e: Exception =>
          logError(s"Failed to update accumulators for $task", e)
      }
    }
  }

  /**
    * Responds to a task finishing. This is called inside the event loop so it assumes that it can
    * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
    */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(stageId, task.partitionId,
      event.taskInfo.attempt, event.reason)

    // The success case is dealt with separately below, since we need to compute accumulator
    // updates before posting.
    if (event.reason != Success) {
      val attemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
      listenerBus.post(SparkListenerTaskEnd(stageId, attemptId, taskType, event.reason,
        event.taskInfo, event.taskMetrics))
    }

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        listenerBus.post(SparkListenerTaskEnd(stageId, stage.latestInfo.attemptId, taskType,
          event.reason, event.taskInfo, event.taskMetrics))
        stage.pendingTasks -= task
        task match {
          case rt: ResultTask[_, _] =>
            stage.resultOfJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(stage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the stage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo("Ignoring possibly bogus ShuffleMapTask completion from " + execId)
            } else {
              stage.addOutputLoc(smt.partitionId, status)
            }
            if (runningStages.contains(stage) && stage.pendingTasks.isEmpty) {
              markStageAsFinished(stage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)
              if (stage.shuffleDep.isDefined) {
                // We supply true to increment the epoch number here in case this is a
                // recomputation of the map outputs. In that case, some nodes may have cached
                // locations with holes (from when we detected the error) and will need the
                // epoch incremented to refetch them.
                // TODO: Only increment the epoch number if this is not the first time
                //       we registered these map outputs.
                mapOutputTracker.registerMapOutputs(
                  stage.shuffleDep.get.shuffleId,
                  stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray,
                  changeEpoch = true)
              }
              clearCacheLocs()
              if (stage.outputLocs.exists(_ == Nil)) {
                // Some tasks had failed; let's resubmit this stage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + stage + " (" + stage.name +
                  ") because some of its tasks had failed: " +
                  stage.outputLocs.zipWithIndex.filter(_._1 == Nil).map(_._2).mkString(", "))
                submitStage(stage)
              } else {
                val newlyRunnable = new ArrayBuffer[Stage]
                for (stage <- waitingStages) {
                  logInfo("Missing parents for " + stage + ": " + getMissingParentStages(stage))
                }
                for (stage <- waitingStages if getMissingParentStages(stage) == Nil) {
                  newlyRunnable += stage
                }
                waitingStages --= newlyRunnable
                runningStages ++= newlyRunnable
                for {
                  stage <- newlyRunnable.sortBy(_.id)
                  jobId <- activeJobForStage(stage)
                } {
                  logInfo("Submitting " + stage + " (" + stage.rdd + "), which is now runnable")
                  submitMissingTasks(stage, jobId)
                }
              }
            }
        }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingTasks += task

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleToMapStage(shuffleId)

        // It is likely that we receive multiple FetchFailed for a single stage (because we have
        // multiple tasks running concurrently on different executors). In that case, it is possible
        // the fetch failure has already been handled by the scheduler.
        if (runningStages.contains(failedStage)) {
          logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
            s"due to a fetch failure from $mapStage (${mapStage.name})")
          markStageAsFinished(failedStage, Some(failureMessage))
        }

        if (disallowStageRetryForTest) {
          abortStage(failedStage, "Fetch failure will not retry stage due to testing config")
        } else if (failedStages.isEmpty) {
          // Don't schedule an event to resubmit failed stages if failed isn't empty, because
          // in that case the event will already have been scheduled.
          // TODO: Cancel running tasks in the stage
          logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
            s"$failedStage (${failedStage.name}) due to fetch failure")
          messageScheduler.schedule(new Runnable {
            override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
          }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
        }
        failedStages += failedStage
        failedStages += mapStage
        // Mark the map whose fetch failed as broken in the map stage
        if (mapId != -1) {
          mapStage.removeOutputLoc(mapId, bmAddress)
          mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
        }

        // TODO: mark the executor as failed only if there were lots of fetch failures on it
        if (bmAddress != null) {
          handleExecutorLost(bmAddress.executorId, fetchFailed = true, Some(task.epoch))
        }

      case commitDenied: TaskCommitDenied =>
      // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case ExceptionFailure(className, description, stackTrace, fullStackTrace, metrics) =>
      // Do nothing here, left up to the TaskScheduler to decide how to handle user failures

      case TaskResultLost =>
      // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case other =>
      // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
      // will abort the job.
    }
    submitWaitingStages()
  }

  /**
    * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
    * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
    *
    * We will also assume that we've lost all shuffle blocks associated with the executor if the
    * executor serves its own blocks (i.e., we're not using external shuffle) OR a FetchFailed
    * occurred, in which case we presume all shuffle data related to this executor to be lost.
    *
    * Optionally the epoch during which the failure was caught can be passed to avoid allowing
    * stray fetch failures from possibly retriggering the detection of a node as lost.
    */
  private[scheduler] def handleExecutorLost(
                                             execId: String,
                                             fetchFailed: Boolean,
                                             maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (!env.blockManager.externalShuffleServiceEnabled || fetchFailed) {
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray
          mapOutputTracker.registerMapOutputs(shuffleId, locs, changeEpoch = true)
        }
        if (shuffleToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
        "(epoch " + currentEpoch + ")")
    }
    submitWaitingStages()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    submitWaitingStages()
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
    submitWaitingStages()
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
    submitWaitingStages()
  }

  /**
    * Marks a stage as finished and removes it from the list of running stages.
    */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo("%s (%s) failed in %s s".format(stage, stage.name, serviceTime))
    }
    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
    * Aborts all jobs depending on a particular Stage. This is called in response to a task set
    * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
    */
  private[scheduler] def abortStage(failedStage: Stage, reason: String) {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason")
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /**
    * Fails a job and all stages that are only used by that job, and cleans up relevant state.
    */
  private def failJobAndIndependentStages(job: ActiveJob, failureReason: String) {
    val error = new SparkException(failureReason)
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try {
              // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
                ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      job.listener.jobFailed(error)
      cleanupStateForJobAndIndependentStages(job)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /**
    * Return true if one of stage's ancestors is target.
    */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    val visitedStages = new HashSet[Stage]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.jobId)
              if (!mapStage.isAvailable) {
                visitedStages += mapStage
                waitingForVisit.push(mapStage.rdd)
              } // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
    * Gets the locality information associated with a partition of a particular RDD.
    * 获取RDD的分区的位置信息<br>
    * This method is thread-safe and is called from both DAGScheduler and SparkContext.
    * <br>这个方法是线程安全的，可以被 DAGScheduler 和 SparkContext调用 .<br>
    *
    * @param rdd       whose partitions are to be looked at
    * @param partition to lookup locality information for
    * @return list of machines that are preferred by the partition
    */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
    * Recursive implementation for getPreferredLocs.
    *
    * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
    * methods (getCacheLocs()); please be careful when modifying this method, because any new
    * DAGScheduler state accessed by it may require additional synchronization.
    */
  private def getPreferredLocsInternal(
                                        rdd: RDD[_],
                                        partition: Int,
                                        visited: HashSet[(RDD[_], Int)])
  : Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (!cached.isEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (!rddPrefs.isEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }
    // If the RDD has narrow dependencies, pick the first partition of the first narrow dep
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }
      case _ =>
    }
    Nil
  }

  def stop() {
    logInfo("Stopping DAGScheduler")
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  // Start the event thread at the end of the constructor
  // TODO 重重之重！！！！  在DAGScheduler主构造器的最后一行代码完成事件循环处理器的启动
  eventProcessLoop.start()
}

/**
  * DAGSchedulerEventProcessLoop : dagScheduler的事件处理器,在DAGSchedulerEventProcessLoop的
  * <br>onReceive(event: DAGSchedulerEvent)方法中完成时间的模式匹配,对相应时间的处理
  * <br>事件类型有:
  *
  * @param dagScheduler
  */
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  /**
    * The main event loop of the DAG scheduler.
    * <br><br>DAG scheduler的事件循环 1:JobSubmitted 2:StageCancelled 3:JobCancelled 等等
    */
  override def onReceive(event: DAGSchedulerEvent): Unit = event match {

    // TODO 提交作业
    case JobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite,
        listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId) =>
      dagScheduler.handleExecutorLost(execId, fetchFailed = false)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion@CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stop()
  }

  override def onStop() {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }

}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
