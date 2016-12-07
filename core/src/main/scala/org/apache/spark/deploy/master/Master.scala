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

import java.io.FileNotFoundException
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

/**
  *
  * @param host
  * @param port
  * @param webUiPort
  * @param securityMgr
  * @param conf
  *
  * Master继承至akka.actor
  *
  * 在主构造器中完成Master配置信息的设置
  *
  */
private[spark] class Master(
                             host: String,
                             port: Int,
                             webUiPort: Int,
                             val securityMgr: SecurityManager,
                             val conf: SparkConf)
  extends Actor with ActorLogReceive with Logging with LeaderElectable {

  import context.dispatcher

  // to use Akka's scheduler.schedule()

  val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  // For application IDs
  val WORKER_TIMEOUT = conf.getLong("spark.worker.timeout", 60) * 1000
  val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")

  /**
    * <br> class WorkerInfo( val id: String, val host: String, val port: Int,val cores: Int,  val memory: Int,val actor: ActorRef, val webUiPort: Int,val publicAddress: String)
    * <br>
    * <br>Master存储的所有worker信息
    *
    */
  val workers = new HashSet[WorkerInfo]
  /**
    * HashMap 存储id到WorkerInfo的映射
    */
  val idToWorker = new HashMap[String, WorkerInfo]

  /**
    * HashMap 存储Address到WorkerInfo的映射
    */
  val addressToWorker = new HashMap[Address, WorkerInfo]

  /**
    * <br>HashSet存储应用信息。
    * <br> ApplicationInfo(val startTime: Long, val id: String,val desc: ApplicationDescription,val submitDate: Date, val driver: ActorRef, defaultCores: Int)
    */
  val apps = new HashSet[ApplicationInfo]

  /**
    * val idToApp = new HashMap[String, ApplicationInfo]
    */
  val idToApp = new HashMap[String, ApplicationInfo]

  /**
    * val actorToApp = new HashMap[ActorRef, ApplicationInfo]
    */
  val actorToApp = new HashMap[ActorRef, ApplicationInfo]
  /**
    * val addressToApp = new HashMap[Address, ApplicationInfo]
    */
  val addressToApp = new HashMap[Address, ApplicationInfo]

  /**
    * val waitingApps = new ArrayBuffer[ApplicationInfo]
    */
  val waitingApps = new ArrayBuffer[ApplicationInfo]

  /**
    * val completedApps = new ArrayBuffer[ApplicationInfo]
    */
  val completedApps = new ArrayBuffer[ApplicationInfo]
  var nextAppNumber = 0
  /**
    * val appIdToUI = new HashMap[String, SparkUI]
    */
  val appIdToUI = new HashMap[String, SparkUI]

  val drivers = new HashSet[DriverInfo]
  /**
    * val completedDrivers = new ArrayBuffer[DriverInfo]
    */
  val completedDrivers = new ArrayBuffer[DriverInfo]
  /**
    *
    * <br>
    * <br>存储 “等待的Driver信息”
    * <br>
    *
    * val waitingDrivers = new ArrayBuffer[DriverInfo]
    *
    *
    */
  val waitingDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  var nextDriverNumber = 0

  Utils.checkHost(host, "Expected hostname")

  val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  val masterSource = new MasterSource(this)

  val webUi = new MasterWebUI(this, webUiPort)

  val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  val masterUrl = "spark://" + host + ":" + port
  var masterWebUiUrl: String = _

  /**
    * Master的状态信息分为：<br>1 ：RecoveryState.ALIVE <br>2：RecoveryState.STANDBY
    */
  var state = RecoveryState.STANDBY


  /**
    * 持久化引擎，完成容错。存储一切需要在时报中恢复时候使用的信息
    */
  var persistenceEngine: PersistenceEngine = _

  var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: Cancellable = _


  /**
    * Round Robin，中文名是轮询调度，是一种服务器计算方法。<br><br><br>
    * As a temporary workaround（工作区） before better ways of configuring memory, we allow users to set
    * a flag that will perform round-robin scheduling across the nodes (spreading out each app
    * among all the nodes) instead of trying to consolidate（巩固，加强） each app onto a small # of nodes.
    * <br><br>
    * spread out :展开；铺开；伸张
    * <br><br>
    *  spark.deploy.spreadOut = true 的话。SpreadOut分配策略是一种以round-robin方式遍历集群所有可用Worker，分配Worker资源，来启动创建Executor的策略，
    * 好处是尽可能的将cores分配到各个节点，最大化负载均衡和高并行。（默认值：Spark默认提供了一种在各个节点进行round-robin的调度）
    * <br><br><br><br>
    *
    * spark.deploy.spreadOut = flase 的话。非SpreadOut会尽可能的根据每个Worker的剩余资源来启动Executor，这样启动的Executor可能只在集群的一小部分机器的Worker上。
    * 这样做对node较少的集群还可以，集群规模大了，Executor的并行度和机器负载均衡就不能够保证了。
    *
    * <br><br>
    * 大白话的意思就是：<br>1）spark.deploy.spreadOut = true  ->Spark作业尽可能地打散分布在集群的各个节点上
    * <br>2）spark.deploy.spreadOut = flase   ->Spark作业尽可能集中分布在集群的某一些节点上
    * <br><br>
    * 引用的网址为：  http://www.csdn123.com/html/topnews201408/43/743.htm
    *
    * spreadOutApps = true  打散<br>
    * spreadOutApps = flase 集中
    *
    */
  val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)

  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private val restServer =
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      Some(new StandaloneRestServer(host, port, self, masterUrl, conf))
    } else {
      None
    }
  private val restServerBoundPort = restServer.map(_.start())

  override def preStart() {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort

    /**
      * 定时器
      */
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    /**
      * 选择持久化引擎，分为ZOOKEEPER、FILESYSTEM、CUSTOM
      */
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, SerializationExtension(context.system))
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Class.forName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(conf.getClass, Serialization.getClass)
          .newInstance(conf, SerializationExtension(context.system))
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("Master actor restarted due to exception", reason)
  }

  override def postStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel()
    }
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self ! ElectedLeader
  }

  override def revokedLeadership() {
    self ! RevokedLeadership
  }

  /**
    *
    * @return
    */
  override def receiveWithLogging = {
    case ElectedLeader => {
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData()
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = context.system.scheduler.scheduleOnce(WORKER_TIMEOUT millis, self,
          CompleteRecovery)
      }
    }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership => {
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    }

    /**
      *
      * Worker发来的注册消息
      *
      *
      */
    case RegisterWorker(id, workerHost, workerPort, cores, memory, workerUiPort, publicAddress) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))

      //如果当前Master状态为RecoveryState.STANDBY ，不回应Worker信息。
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        //如果包含WorkerInfo了，回复注册失败信息
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {

        //注册新的Worker信息
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory, sender, workerUiPort, publicAddress)
        if (registerWorker(worker)) {


          //完成worker的持久化，以防master宕机之后无法恢复
          persistenceEngine.addWorker(worker)

          //给Worker发送消息：告诉worker完成注册RegisteredWorker
          sender ! RegisteredWorker(masterUrl, masterWebUiUrl)
          schedule()
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress)
        }
      }
    }

    case RequestSubmitDriver(description) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only accept driver submissions in ALIVE state. Current state: $state."
        sender ! SubmitDriverResponse(false, None, msg)
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        sender ! SubmitDriverResponse(true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}")
      }
    }

    case RequestKillDriver(driverId) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only kill drivers in ALIVE state. Current state: $state."
        sender ! KillDriverResponse(driverId, success = false, msg)
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self ! DriverStateChanged(driverId, DriverState.KILLED, None)
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.actor ! KillDriver(driverId)
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            sender ! KillDriverResponse(driverId, success = true, msg)
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            sender ! KillDriverResponse(driverId, success = false, msg)
        }
      }
    }

    case RequestDriverStatus(driverId) => {
      (drivers ++ completedDrivers).find(_.id == driverId) match {
        case Some(driver) =>
          sender ! DriverStatusResponse(found = true, Some(driver.state),
            driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception)
        case None =>
          sender ! DriverStatusResponse(found = false, None, None, None, None)
      }
    }

    /**
      * 提交应用给Master，Master启动executor
      *
      * <br>（如果没有理解错误的话）description中的command应该是：val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend"，其余参数略）
      * 代码位置：类的 SparkDeploySchedulerBackend中的command
      *
      */
    case RegisterApplication(description) => {
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        //TODO 把应用信息存到内存, 重点：sender应该是clientActor
        val app = createApplication(description, sender) //sender应该是clientActor

        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        //持久化app，实现容错
        persistenceEngine.addApplication(app)
        //回复appClient已经注册（这一块不是worker）
        sender ! RegisteredApplication(app.id, masterUrl)
        //TODO Master开始调度资源，其实就是把任务启动启动到哪些Worker上
        schedule()
      }
    }
    //TODO appClient发送来的消息，通知Executor状态
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {

      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) => {
          val appInfo = idToApp(appId)
          exec.state = state
          if (state == ExecutorState.RUNNING) {
            appInfo.resetRetryCount()
          }
          // exec.application.driver = driverClient
          exec.application.driver ! ExecutorUpdated(execId, state, message, exitStatus)

//          完成状态包括：KILLED, FAILED, LOST, EXITED 注意：这里是完成，不是成功！
          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            appInfo.removeExecutor(exec)//appInfo移除executor
            exec.worker.removeExecutor(exec)//worker移除executor

            val normalExit = exitStatus == Some(0) //判断是否正常推出
            // Only retry certain number of times so we don't go into an infinite loop.
            if (!normalExit) {
              //异常退出
              if (appInfo.incrementRetryCount() < ApplicationState.MAX_NUM_RETRY) {
                //当前重试次数是否小于最大重试次数MAX_NUM_RETRY10，如果小于重新调度
                schedule()
              } else {
                //超过最大重启次数
                val execs = appInfo.executors.values//获取当前app的所有executors
                if (!execs.exists(_.state == ExecutorState.RUNNING)) {//如果不存在运行的executor的话，直接removeApplication
                  logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                    s"${appInfo.retryCount} times; removing it")
                  removeApplication(appInfo, ApplicationState.FAILED)
                }
              }
            }
          }
        }
          //位置状态
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }
    }

    /**
      * Worker发送来的消息，告诉Driver当前worker状态
      *
      */
    case DriverStateChanged(driverId, state, exception) => {
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }
    }

    case Heartbeat(workerId) => {
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            sender ! ReconnectWorker(masterUrl)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }
    }

    case MasterChangeAcknowledged(appId) => {
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) {
        completeRecovery()
      }
    }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) => {
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.drivers(driverId) = driver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) {
        completeRecovery()
      }
    }

    case DisassociatedEvent(_, address, _) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      logInfo(s"$address got disassociated, removing it.")
      addressToWorker.get(address).foreach(removeWorker)
      addressToApp.get(address).foreach(finishApplication)
      if (state == RecoveryState.RECOVERING && canCompleteRecovery) {
        completeRecovery()
      }
    }

    case RequestMasterState => {
      sender ! MasterStateResponse(
        host, port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state)
    }

    case CheckForWorkerTimeOut => {
      timeOutDeadWorkers()
    }

    case BoundPortsRequest => {
      sender ! BoundPortsResponse(port, webUi.boundPort, restServerBoundPort)
    }
  }

  def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
                    storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.actor ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    synchronized {
      if (state != RecoveryState.RECOVERING) {
        return
      }
      state = RecoveryState.COMPLETING_RECOVERY
    }

    // Kill off any workers and apps that didn't respond to us.
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
    * Can an app use the given worker? True if the worker has enough memory and we haven't already
    * launched an executor for the app on it (right now the standalone backend doesn't like having
    * two executors on the same worker).
    *
    * 判断应用是否可以使用这个worker？
    * <br><br>
    * 判断依据：
    * <br>1：该worker拥有足够的内存
    * <br>2：该app还没有在这个worker上启动executor<br>
    * <br>注意：<br>
    * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;目前standalone backend 对于同一个app不会在相同的Worker上启动两个executor
    * （但是：不同的app可以在该worker上分别启动属于自己的一个executor（目前新版本已经没有这个限制了））
    *
    */
  def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }

  /**
    * Schedule the currently available resources among waiting apps. This method will be called
    * every time a new app joins or resource availability changes.
    *
    * <br>调度器：
    * <br>  调度当前等待的apps分配可获取的资源。
    * <br>  当每次有新的app加入或者有可获得资源这时候这个方法被调用
    * <br>两种调度方式（个人称之为）：1）多餐少食 2）暴饮暴食
    * <br>
    * <br>schedule()被调用的时机：
    * <br>1）：当有新的worker（计算资源）加入或者下线时候，schedule()被调用
    * <br>2）：添加app或者移除app，schedule()被调用
    * <br>
    */
  private def schedule() {

    //如果
    if (state != RecoveryState.ALIVE) {
      return
    }

    // First schedule drivers, they take strict precedence（优先；居先） over applications
    // Randomization（ 随机化，不规则分布；随机选择） helps balance drivers
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE)) //在workers中选择处于ALIVE状态的worker
    //求存活的worker数量
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0 //worker的当前标号

    for (driver <- waitingDrivers.toList) {
      //workers 是spark集群的slave节点（worker节点）
      // iterate over a copy of waitingDrivers  迭代waitingDrivers的一个副本
      // We assign workers to each waiting driver in a round-robin fashion. For each driver,    我们制定workers给每一个等待的Driver
      // we  start from the last worker that was assigned a driver, and continue onwards(向前) until we have  我们冲上一次指定的worker位置开始继续向前指定
      // explored all alive workers.//知道所有存活的workers都指定完毕
      var launched = false
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        //当前遍历的worker如果满足内存大于提交应用需要的内存和满足需要的core数目的话，
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {

          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }


    //上面代码是兼容老的，下面代码是新的
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    //多餐少食的原理
    if (spreadOutApps) {
      // Try to spread out each app among all the nodes, until it has all its cores
      //尽可能的把每一个app作业分布在集群的所有节点上，在分配过程知道得到该有的Cpu核数为止

      //if app.coresLeft > 0 作用：应用需要的核数 没分配一次减少一次，只要还有核没分配的话，就再次循环分配
      for (app <- waitingApps if app.coresLeft > 0) {
        //app在waitingApps

        //过滤掉DEAD状态的Worker（因为Worker超时不立马移除掉，而是修改为Dead状态，之后重试连接如果还没有连接则移除）
        //过滤掉可以使用的worker（过滤掉条件：内存满足，而且没在改worker上启动executor），最后按照核心数排序
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(canUse(app, _)).sortBy(_.coresFree).reverse //


        //usableWorkers为可用的worker数组
        /**
          * numUsable可用的worker数
          */
        val numUsable = usableWorkers.length

        //分配numUsable大小的数组
        /**
          * 每一可用worker（次序）分配出去的核心数存储在数组对应位置的元素中
          */
        val assigned = new Array[Int](numUsable) // Number of cores to give on each node


        /**
          * 求 “numUsable”与“所有可用worker的核心数的总和”的最小值
          * 这么做的目的：
          * <br>1：当可用worker的所有核心数总和小于app所需要的核心数时候，那么只能把所有的可用核心数全给该app了（尽管可用的核心数不够）<br>
          * <br>2：当app所需要的核心数目小于“所有可用worker的核心数的总和”时候，直接按需分配<br>
          * <br><br>
          * <br>等待需要分配的核数
          */
        var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum) //即将要分配的核数


        var pos = 0
        //轮流分配核数，每一轮每一个worker分配出去一个核心。多次循环知道分配完毕
        while (toAssign > 0) {
          //当前该worker（usableWorkers(pos)）已经分配出去assigned(pos)个核心数，如果还有的话继续分配
          //如果该worker没有的话，pos向后移动，换下一个worker分配
          if (usableWorkers(pos).coresFree - assigned(pos) > 0) {
            toAssign -= 1
            assigned(pos) += 1
          }
          pos = (pos + 1) % numUsable
        }

        // Now that we've decided how many cores to give on each node, let's actually give them
        for (pos <- 0 until numUsable) {
          if (assigned(pos) > 0) {
            //该usableWorkers(pos)分配出去了assigned(pos)个核心数
            val exec = app.addExecutor(usableWorkers(pos), assigned(pos))
            //TODO Master发送消息给Worker去启动executor
            //启动worker上的executor
            launchExecutor(usableWorkers(pos), exec)
            //标记运行状态
            app.state = ApplicationState.RUNNING
          }
        }
      }
    } else {
      // Pack each app into as few nodes as possible until we've assigned all its cores
      //尽可能集中在一些节点上

      //集中式分配资源，有点饿汉式感觉，现有多少吃多少(暴饮暴食)
      for (worker <- workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        //一般应用再完成或者故障失败才在waitingApps移除，所以没完成之前一直在waitingApps中
        for (app <- waitingApps if app.coresLeft > 0) {
          //此循环一直运行，直到app失败或者完成 或者app.coresLeft <=0时候停止循环
          if (canUse(app, worker)) {
            val coresToUse = math.min(worker.coresFree, app.coresLeft)
            if (coresToUse > 0) {
              val exec = app.addExecutor(worker, coresToUse)
              launchExecutor(worker, exec)
              app.state = ApplicationState.RUNNING
            }
          }
        }
      }
    }
  }

  /**
    *
    * @param worker worker的节点信息（哪一个worker）
    * @param exec   ExecutorDesc包含信息
    *               <br><br><br> id Executor的编号、application - 属于哪一个应用、worker - 属于哪一个Worker、cores - 核心数、memory - 内存
    */
  def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc) {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    //TODO 记录executor使用当前Worker的资源
    worker.addExecutor(exec)

    //worker.actor是worker的actor引用
    //TODO master 给worker发送消息让其启动Executor
    worker.actor ! LaunchExecutor(masterUrl, exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory)

    //给driver反馈消息，说executor启动完毕（准确来说是给clientActor发消息）
    //TODO  master向clientActor（注意不是driverActor，clientActor与driverActor都运行在sparksubmit中）发送消息，通知executor启动完毕
    exec.application.driver ! ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory)
  }

  /**
    *
    * @param worker
    * @return
    */
  def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)

      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        //译文：一个worker的注册状态为UNKNOWN意味着在恢复过程中从新注册，所以此时需要移除oldWorker
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    //如果Worker为DEAD状态会在workers移除
    //如果Worker为UNKNOWN状态 也会移除
    //最后只剩下激活状态了
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver ! ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None)
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    persistenceEngine.removeWorker(worker)
  }

  def relaunchDriver(driver: DriverInfo) {
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    waitingDrivers += driver
    schedule()
  }

  /**
    *
    * @param desc
    * @param driver
    * @return 返回一个ApplicationInfo
    */
  def createApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    //TODO driver是clientActor
    new ApplicationInfo(now, newApplicationId(date), desc, date, driver, defaultCores)
  }

  /**
    * <br>注册应用信息
    *
    * <br>如果应用在addressToApp映射中的话，跳过否则加到容器中
    * <br>
    *
    * @param app ：ApplicationInfo  要注册的应用信息
    */
  def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.path.address

    //
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    //将应用信息存储到容器中
    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    actorToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
  }

  def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  /**
    * 移除app
    *
    * @param app
    * @param state
    */
  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      actorToApp -= app.driver
      addressToApp -= app.driver.path.address
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach(a => {
          appIdToUI.remove(a.id).foreach { ui => webUi.detachSparkUI(ui) }
          applicationMetricsSystem.removeSource(a.appSource)
        })
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      // If application events are logged, use them to rebuild the UI
      rebuildSparkUI(app)

      for (exec <- app.executors.values) {
        exec.worker.removeExecutor(exec)
        exec.worker.actor ! KillExecutor(masterUrl, exec.application.id, exec.id)
        exec.state = ExecutorState.KILLED
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver ! ApplicationRemoved(state.toString)
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.actor ! ApplicationFinished(app.id)
      }
    }
  }

  /**
    * Rebuild a new SparkUI from the given application's event logs.
    * Return whether this is successful.
    */
  def rebuildSparkUI(app: ApplicationInfo): Boolean = {
    val appName = app.desc.name
    val notFoundBasePath = HistoryServer.UI_PATH_PREFIX + "/not-found"
    try {
      val eventLogFile = app.desc.eventLogDir
        .map { dir => EventLoggingListener.getLogPath(dir, app.id, app.desc.eventLogCodec) }
        .getOrElse {
          // Event logging is not enabled for this application
          app.desc.appUiUrl = notFoundBasePath
          return false
        }

      val fs = Utils.getHadoopFileSystem(eventLogFile, hadoopConf)

      if (fs.exists(new Path(eventLogFile + EventLoggingListener.IN_PROGRESS))) {
        // Event logging is enabled for this application, but the application is still in progress
        val title = s"Application history not found (${app.id})"
        var msg = s"Application $appName is still in progress."
        logWarning(msg)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        return false
      }

      val logInput = EventLoggingListener.openEventLog(new Path(eventLogFile), fs)
      val replayBus = new ReplayListenerBus()
      val ui = SparkUI.createHistoryUI(new SparkConf, replayBus, new SecurityManager(conf),
        appName + " (completed)", HistoryServer.UI_PATH_PREFIX + s"/${app.id}")
      try {
        replayBus.replay(logInput, eventLogFile)
      } finally {
        logInput.close()
      }
      appIdToUI(app.id) = ui
      webUi.attachSparkUI(ui)
      // Application UI is successfully rebuilt, so link the Master UI to it
      app.desc.appUiUrl = ui.basePath
      true
    } catch {
      case fnf: FileNotFoundException =>
        // Event logging is enabled for this application, but no event logs are found
        val title = s"Application history not found (${app.id})"
        var msg = s"No event logs found for application $appName in ${app.desc.eventLogDir}."
        logWarning(msg)
        msg += " Did you specify the correct logging directory?"
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        false
      case e: Exception =>
        // Relay exception message to application UI page
        val title = s"Application history load error (${app.id})"
        val exception = URLEncoder.encode(Utils.exceptionString(e), "UTF-8")
        var msg = s"Exception in replaying log for application $appName!"
        logError(msg, e)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&exception=$exception&title=$title"
        false
    }
  }

  /** Generate a new app ID given a app's submission date */
  def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT / 1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  /**
    *
    * @param worker 分配给该Driver的worker
    * @param driver Driver信息 ，当前提交应用的Driver信息
    */
  def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    //把Driver信息保存到worker中
    worker.addDriver(driver)

    driver.worker = Some(worker)
    //发送消息给worker
    worker.actor ! LaunchDriver(driver.id, driver.desc)

    driver.state = DriverState.RUNNING
  }

  def removeDriver(driverId: String, finalState: DriverState, exception: Option[Exception]) {
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(w => w.removeDriver(driver))
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[spark] object Master extends Logging {
  val systemName = "sparkMaster"
  private val actorName = "Master"

  def main(argStrings: Array[String]) {

    SignalLogger.register(log)
    //参数配置准备
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    //创建actorSystem
    val (actorSystem, _, _, _) = startSystemAndActor(args.host, args.port, args.webUiPort, conf)
    actorSystem.awaitTermination()
  }

  /**
    * Returns an `akka.tcp://...` URL for the Master actor given a sparkUrl `spark://host:port`.
    *
    * @throws SparkException if the url is invalid
    */
  def toAkkaUrl(sparkUrl: String, protocol: String): String = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    AkkaUtils.address(protocol, systemName, host, port, actorName)
  }

  /**
    * Returns an akka `Address` for the Master actor given a sparkUrl `spark://host:port`.
    *
    * @throws SparkException if the url is invalid
    */
  def toAkkaAddress(sparkUrl: String, protocol: String): Address = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    Address(protocol, systemName, host, port)
  }

  /**
    * <br> Start the Master and return a four tuple of:
    * <br>   (1) The Master actor system
    * <br>   (2) The bound port
    * <br>  (3) The web UI bound port
    * <br>   (4) The REST server bound port, if any
    *
    * <br> 创建actorSystem，并在其下创建名字为Master的actor
    */
  def startSystemAndActor(
                           host: String,
                           port: Int,
                           webUiPort: Int,
                           conf: SparkConf): (ActorSystem, Int, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    //创建actorSystem，actorSystem名字为：sparkMaster
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf = conf, securityManager = securityMgr)

    //actorName =Master，在actorSystem下创建一个actor，名字为：Master
    //此处会执行Master的主构造器中的代码
    val actor = actorSystem.actorOf(Props(classOf[Master], host, boundPort, webUiPort, securityMgr, conf), actorName)

    //设置超时时间
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort, portsResponse.restPort)
  }
}
