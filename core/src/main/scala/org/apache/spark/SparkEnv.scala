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

import java.io.File
import java.net.Socket

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Properties

import akka.actor._
import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.scheduler.{OutputCommitCoordinator, LiveListenerBus}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorActor
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleMemoryManager, ShuffleManager}
import org.apache.spark.storage._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
  * :: DeveloperApi ::
  * Holds all the runtime environment objects for a running Spark instance (either master or worker),
  * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
  * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
  * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
  *
  * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
  * in a future release.
  */


/**
  *
  * <br>持有一个spark实例的所有的运行时环境变量对象，包含[[serializer, Akka actor system, block manager, map output tracker]]等等
  * <br>Spark 代码通过全局变量查找到SparkEnv，因此所有的线程都可以访问相同的Spark环境变量
  * <br>可以使用 SparkEnv.get 获取
  *
  * @param executorId
  * @param actorSystem
  * @param serializer
  * @param closureSerializer
  * @param cacheManager
  * @param mapOutputTracker
  * @param shuffleManager
  * @param broadcastManager
  * @param blockTransferService
  * @param blockManager
  * @param securityManager
  * @param httpFileServer
  * @param sparkFilesDir
  * @param metricsSystem
  * @param shuffleMemoryManager
  * @param outputCommitCoordinator
  * @param conf
  */
@DeveloperApi
class SparkEnv(
                val executorId: String,
                val actorSystem: ActorSystem,
                val serializer: Serializer,
                val closureSerializer: Serializer,
                val cacheManager: CacheManager,
                val mapOutputTracker: MapOutputTracker,
                val shuffleManager: ShuffleManager,
                val broadcastManager: BroadcastManager,
                val blockTransferService: BlockTransferService,
                val blockManager: BlockManager,
                val securityManager: SecurityManager,
                val httpFileServer: HttpFileServer,
                val sparkFilesDir: String,
                val metricsSystem: MetricsSystem,
                val shuffleMemoryManager: ShuffleMemoryManager,
                val outputCommitCoordinator: OutputCommitCoordinator,
                val conf: SparkConf) extends Logging {

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private[spark] def stop() {
    isStopped = true
    pythonWorkers.foreach { case (key, worker) => worker.stop() }
    Option(httpFileServer).foreach(_.stop())
    mapOutputTracker.stop()
    shuffleManager.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    metricsSystem.stop()
    outputCommitCoordinator.stop()
    actorSystem.shutdown()
    // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
    // down, but let's call it anyway in case it gets fixed in a later release
    // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
    // actorSystem.awaitTermination()

    // Note that blockTransferService is stopped by BlockManager since it is started by it.
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver"
  private[spark] val executorActorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
    * Returns the SparkEnv.
    */
  def get: SparkEnv = {
    env
  }

  /**
    * Returns the ThreadLocal SparkEnv.
    */
  @deprecated("Use SparkEnv.get instead", "1.2")
  def getThreadLocal: SparkEnv = {
    env
  }

  /**
    *
    * <br>SparkEnv：
    * <br>Create a SparkEnv for the driver.
    * <br>为Driver创建一个SparkEnv
    *
    */
  private[spark] def createDriverEnv(
                                      conf: SparkConf,
                                      isLocal: Boolean,
                                      listenerBus: LiveListenerBus,
                                      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val hostname = conf.get("spark.driver.host")
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      hostname,
      port,
      isDriver = true,
      isLocal = isLocal,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
    * Create a SparkEnv for an executor.
    * In coarse-grained mode, the executor provides an actor system that is already instantiated.
    */
  private[spark] def createExecutorEnv(
                                        conf: SparkConf,
                                        executorId: String,
                                        hostname: String,
                                        port: Int,
                                        numCores: Int,
                                        isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      port,
      isDriver = false,
      isLocal = isLocal,
      numUsableCores = numCores
    )
    SparkEnv.set(env)
    env
  }


  /**
    * <br> Helper method to create a SparkEnv for a driver or an executor.
    * <br> 工具方法为 driver 或者 executor 创建一个SparkEnv
    *
    * @param conf
    * @param executorId
    * @param hostname
    * @param port
    * @param isDriver 是否是Driver
    * @param isLocal
    * @param listenerBus
    * @param numUsableCores
    * @param mockOutputCommitCoordinator
    * @return
    */
  private def create(
                      conf: SparkConf,
                      executorId: String,
                      hostname: String,
                      port: Int,
                      isDriver: Boolean,
                      isLocal: Boolean,
                      listenerBus: LiveListenerBus = null,
                      numUsableCores: Int = 0,
                      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    val securityManager = new SecurityManager(conf)

    // Create the ActorSystem for Akka and get the port it binds to.
    /**
      * 创建一个actorSystemName和获取绑定的端口
      */
    val (actorSystem, boundPort) = {
      //private[spark] val driverActorSystemName = "sparkDriver" driverActorSystemName就是Driver的actorSystem的名字
      val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
      AkkaUtils.createActorSystem(actorSystemName, hostname, port, conf, securityManager)
    }

    // Figure out which port Akka actually bound to in case the original port is 0 or occupied.
    //设置端口
    if (isDriver) {
      conf.set("spark.driver.port", boundPort.toString)
    } else {
      conf.set("spark.executor.port", boundPort.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    /**
      *
      * <br>创建全类名为className的一个实例（使用反射创建实例），可能使用sparkconf完成初始化
      * <br>
      *
      * @param className
      * @tparam T
      * @return
      */
    def instantiateClass[T](className: String): T = {
      val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    /**
      * <br>例如：("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      * <br>propertyName = spark.serializer 获取的值为 org.apache.spark.serializer.JavaSerializer的一个实例
      *
      * @param propertyName     属性名字
      * @param defaultClassName 类名字
      * @tparam T defaultClassName的类型
      * @return 范围T的一个实例
      */
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    /**
      *
      */
    val serializer = instantiateClassFromConf[Serializer]("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    /**
      *
      * <br>Spark序列化器：
      * <br>1：KryoSerializer
      * <br>2：JavaSerializer(默认实现)
      * 创建一个闭包序列化器
      *  @return  KryoSerializer或JavaSerializer的class字节码
      */
    val closureSerializer = instantiateClassFromConf[Serializer]("spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")

    def registerOrLookup(name: String, newActor: => Actor): ActorRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        actorSystem.actorOf(Props(newActor), name = name)
      } else {
        AkkaUtils.makeDriverRef(name, conf, actorSystem)
      }
    }


    val mapOutputTracker = if (isDriver) {
      //MapOutputTracker for the driver. This uses TimeStampedHashMap to keep track of map output information,
      // which allows old output information based on a TTL.
      //译文：为驱动创建一个map输出跟踪器，使用TimeStampedHashMap（时间戳HashMap）记录map的输出信息,基于TTL（time to live:生存时间）
      // 存储旧的map输出信息
      new MapOutputTrackerMaster(conf)
    } else {
      //MapOutputTracker for the executors, which fetches map output
      // information from the driver's MapOutputTrackerMaster.
      //为executors创建一个Map输出的跟踪器，他可以从Driver的map输出跟踪器获取输出信息
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerActor after initialization as MapOutputTrackerActor requires the MapOutputTracker itself
    //
    mapOutputTracker.trackerActor = registerOrLookup(
      "MapOutputTracker",
      new MapOutputTrackerMasterActor(mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    val shuffleMemoryManager = new ShuffleMemoryManager(conf)

    /**
      * 块传输服务：默认使用的是netty
      */
    val blockTransferService =
      conf.get("spark.shuffle.blockTransferService", "netty").toLowerCase match {
        case "netty" =>
          new NettyBlockTransferService(conf, securityManager, numUsableCores)
        case "nio" =>
          new NioBlockTransferService(conf, securityManager)
      }

    val blockManagerMaster = new BlockManagerMaster(registerOrLookup(
      "BlockManagerMaster",
      new BlockManagerMasterActor(isLocal, conf, listenerBus)), conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster,
      serializer, conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager,
      numUsableCores)

    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    val cacheManager = new CacheManager(blockManager)

    val httpFileServer =
      if (isDriver) {
        val fileServerPort = conf.getInt("spark.fileserver.port", 0)
        val server = new HttpFileServer(conf, securityManager, fileServerPort)
        server.initialize()
        conf.set("spark.fileserver.uri", server.serverUri)
        server
      } else {
        null
      }

    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    } else {
      "."
    }

    // Warn about deprecated spark.cache.class property
    if (conf.contains("spark.cache.class")) {
      logWarning("The spark.cache.class property is no longer being used! Specify storage " +
        "levels using the RDD.persist() method instead.")
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf)
    }
    val outputCommitCoordinatorActor = registerOrLookup("OutputCommitCoordinator",
      new OutputCommitCoordinatorActor(outputCommitCoordinator))
    outputCommitCoordinator.coordinatorActor = Some(outputCommitCoordinatorActor)

    new SparkEnv(
      executorId,
      actorSystem,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockTransferService,
      blockManager,
      securityManager,
      httpFileServer,
      sparkFilesDir,
      metricsSystem,
      shuffleMemoryManager,
      outputCommitCoordinator,
      conf)
  }

  /**
    * Return a map representation of jvm information, Spark properties, system properties, and
    * class paths. Map keys define the category, and map values represent the corresponding
    * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
    */
  private[spark]
  def environmentDetails(
                          conf: SparkConf,
                          schedulingMode: String,
                          addedJars: Seq[String],
                          addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
    if (!conf.contains("spark.scheduler.mode")) {
      Seq(("spark.scheduler.mode", schedulingMode))
    } else {
      Seq[(String, String)]()
    }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
