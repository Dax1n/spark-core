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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import scala.collection.mutable
import scala.concurrent.Await

import akka.actor.{Actor, ActorSelection, Props}
import akka.pattern.Patterns
import akka.remote.{RemotingLifecycleEvent, DisassociatedEvent}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}


/**
  *
  * CoarseGrainedExecutorBackend，粗粒度的ExecutorBackend进程。<br>
  * Worker为什么要启动另外一个进程？<br>
  * Worker本身是管理当前机器上的资源，变动资源的时候向Master汇报。有很多应用程序，就需要很多Executor。这样程序之间不会一个奔溃导致所有的都奔溃。<br>
  *1.在CoarseGrainedExecutorBackend启动时，向Driver注册Executor其实质是注册ExecutorBackend实例，和Executor实例之间没有直接的关系！！！<br>
  *2.CoarseGrainedExecutorBackend是Executor运行所在的进程名称，Executor才是真正在处理Task的对象，Executor内部是通过线程池的方式来完成Task的计算的。<br>
  *3. CoarseGrainedExecutorBackend和Executor是一一对应的。<br>
  *4. CoarseGrainedExecutorBackend是一个消息通信体（其实现了ThreadSafeRpcEndpoint）。可以发送信息给Driver，并可以接收Driver中发过来的指令，例如启动Task等。<br>
  *5.在Driver进程中，有两个至关重要的Endpoint，<br>
  * a）第一个就是ClientEndpoint，主要负责向Master注册当前的程序；是AppClient的内部成员。<br>
  * b）另外一个就是DriverEndpoint，这是整个程序运行时候的驱动器！！是CoarseGrainedExecutorBackend的内部成员。<br>
  *6.在Driver中通过ExecutorData封装并注册ExecutorBackend的信息到Driver的内存数据结构ExecutorMapData中。ExecutorMapData是CoarseGrainedSchedulerBackend的成员。<br>最终是注册给CoarseGrainedSchedulerBackend。
  * <br>
  * <br>Executor用来发送更新给集群调度器的可插拔接口
  * <br>
  * <br>CoarseGrainedExecutorBackend继承关系：
  * <br>
  * <br>class CoarseGrainedExecutorBackend extends Actor with ActorLogReceive with ExecutorBackend with Logging
  *
  * @param driverUrl
  * @param executorId
  * @param hostPort
  * @param cores
  * @param userClassPath
  * @param env
  */
private[spark] class CoarseGrainedExecutorBackend(
                                                   driverUrl: String,
                                                   executorId: String,
                                                   hostPort: String,
                                                   cores: Int, userClassPath: Seq[URL], env: SparkEnv)
  extends Actor with ActorLogReceive with ExecutorBackend with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  var driver: ActorSelection = null

  /**
    * CoarseGrainedExecutorBackend生命周期方法：给DriverActor发送消息<br>
    *
    */
  override def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    driver = context.actorSelection(driverUrl)
    //向Driver发送消息已经注册Executor， RegisterExecutor
    driver ! RegisterExecutor(executorId, hostPort, cores, extractLogUrls)
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
  }

  override def receiveWithLogging = {
    //TODO DriverActor给发来的消息，告诉CoarseGrainedExecutorBackend已经完成了Executor的注册
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")

      val (hostname, _) = Utils.parseHostPort(hostPort)
      //
      executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        val ser = env.closureSerializer.newInstance()
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        logError("Received KillTask command but executor was null")
        System.exit(1)
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case x: DisassociatedEvent =>
      if (x.remoteAddress == driver.anchorPath.address) {
        logError(s"Driver $x disassociated! Shutting down.")
        System.exit(1)
      } else {
        logWarning(s"Received irrelevant DisassociatedEvent $x")
      }

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      executor.stop()
      context.stop(self)
      context.system.shutdown()
  }

  /**
    * 给driverActor更新状态
    *
    * @param taskId
    * @param state
    * @param data
    */
  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

  private def run(
                   driverUrl: String,
                   executorId: String,
                   hostname: String,
                   cores: Int,
                   appId: String,
                   workerUrl: Option[String],
                   userClassPath: Seq[URL]) {

    SignalLogger.register(log)

    //
    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap（引导程序，辅助程序；） to fetch the driver's Spark properties.
      //引导获取spark driver的配置信息
      val executorConf = new SparkConf//完成默认的spark系统配置加载

      val port = executorConf.getInt("spark.executor.port", 0)

      //TODO 在Executor中创建actorSystem
      val (fetcher, _) = AkkaUtils.createActorSystem("driverPropsFetcher", hostname, port, executorConf, new SecurityManager(executorConf))


      //TODO driver引用，与driver建立连接
      val driver = fetcher.actorSelection(driverUrl)


      val timeout = AkkaUtils.askTimeout(executorConf)
      val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
      val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]] ++
        Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      val env = SparkEnv.createExecutorEnv(
        driverConf, executorId, hostname, port, cores, isLocal = false)

      // SparkEnv sets spark.driver.port so it shouldn't be 0 anymore.
      val boundPort = env.conf.getInt("spark.executor.port", 0)
      assert(boundPort != 0)

      // Start the CoarseGrainedExecutorBackend actor.
      val sparkHostPort = hostname + ":" + boundPort

      //TODO CoarseGrainedExecutorBackend真正进行通信的actor
      env.actorSystem.actorOf(Props(classOf[CoarseGrainedExecutorBackend], driverUrl, executorId, sparkHostPort, cores, userClassPath, env), name = "Executor")

      //TODO 监督worker的actor
      workerUrl.foreach { url => env.actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
      }
      env.actorSystem.awaitTermination()
    }
  }//run结束

  /**
    *
    * 使用命令行启动java子进程：CoarseGrainedExecutorBackend
    * <br>
    * <br>
    *
    * @param args
    */
  def main(args: Array[String]) {
    //TODO executor进程执行的入口，如果想debug executor在此处
    var driverUrl: String = null //worker的applicationDescription包含driverActor
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    //把传入的参数转为list
    var argv = args.toList

    //TODO 遍历executor参数
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }
    //TODO 调用run方法，driverUrl 为driverActor
    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
  }

  private def printUsageAndExit() = {
    System.err.println(
      """
        |"Usage: CoarseGrainedExecutorBackend [options]
        |
        | Options are:
        |   --driver-url <driverUrl>
        |   --executor-id <executorId>
        |   --hostname <hostname>
        |   --cores <cores>
        |   --app-id <appid>
        |   --worker-url <workerUrl>
        |   --user-class-path <url>
        | """.stripMargin)
    System.exit(1)
  }

}
