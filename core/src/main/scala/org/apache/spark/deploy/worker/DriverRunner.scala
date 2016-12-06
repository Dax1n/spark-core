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

package org.apache.spark.deploy.worker

import java.io._

import scala.collection.JavaConversions._

import akka.actor.ActorRef
import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files
import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.util.{Clock, SystemClock}

/**
  * Manages the execution of one driver, including automatically（自动） restarting the driver on failure.
  * This is currently only used in standalone cluster deploy mode.
  * <br><br>
  * &nbsp;&nbsp;&nbsp;管理driver的执行，包括自动失败重启
  *
  *
  */
private[spark] class DriverRunner(
                                   val conf: SparkConf,
                                   val driverId: String,
                                   val workDir: File,
                                   val sparkHome: File,
                                   val driverDesc: DriverDescription,
                                   val worker: ActorRef,
                                   val workerUrl: String) extends Logging {

  @volatile var process: Option[Process] = None
  @volatile var killed = false

  // Populated once finished
  var finalState: Option[DriverState] = None
  var finalException: Option[Exception] = None
  var finalExitCode: Option[Int] = None

  // Decoupled for testing
  private[deploy] def setClock(_clock: Clock) = clock = _clock

  private[deploy] def setSleeper(_sleeper: Sleeper) = sleeper = _sleeper

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile(f => {
      Thread.sleep(1000); !killed
    })
  }

  /**
    * Starts a thread to run and manage the driver.
    * <br>
    * 启动一个线程去运行和管理driver
    *
    *
    *
    */
  def start() = {
    //创建一个线程
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        try {
          //准备驱动的工作目录
          val driverDir = createWorkingDirectory()

          //
          val localJarFilename = downloadUserJar(driverDir)

          def substituteVariables(argument: String): String = argument match {
            case "{{WORKER_URL}}" => workerUrl
            case "{{USER_JAR}}" => localJarFilename
            case other => other
          }

          // TODO: If we add ability to submit multiple jars they should also be added here
          val builder = CommandUtils.buildProcessBuilder(driverDesc.command, driverDesc.mem,
            sparkHome.getAbsolutePath, substituteVariables)
          launchDriver(builder, driverDir, driverDesc.supervise)
        }
        catch {
          case e: Exception => finalException = Some(e)
        }

        val state =
          if (killed) {
            DriverState.KILLED
          } else if (finalException.isDefined) {
            DriverState.ERROR
          } else {
            finalExitCode match {
              case Some(0) => DriverState.FINISHED
              case _ => DriverState.FAILED
            }
          }

        finalState = Some(state)

        //发送消息给Master
        worker ! DriverStateChanged(driverId, state, finalException)
      }
    }.start()
  }//start 结束



  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  def kill() {
    synchronized {
      process.foreach(p => p.destroy())
      killed = true
    }
  }

  /**
    * Creates the working directory for this driver.
    * Will throw an exception if there are errors preparing the directory.
    * <br>
    * 为driver创建一个工作目录
    *  @return File
    */

  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
    * Download the user jar into the supplied directory and return its local path.
    * Will throw an exception if there are errors downloading the jar.
    * <br>
    *   下载用户开发的jar包到提供的目录（工作目录）
    */
  private def downloadUserJar(driverDir: File): String = {

    val jarPath = new Path(driverDesc.jarUrl)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val jarFileSystem = jarPath.getFileSystem(hadoopConf)

    val destPath = new File(driverDir.getAbsolutePath, jarPath.getName)
    val jarFileName = jarPath.getName
    val localJarFile = new File(driverDir, jarFileName)
    val localJarFilename = localJarFile.getAbsolutePath

    if (!localJarFile.exists()) {
      // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar $jarPath to $destPath")
      FileUtil.copy(jarFileSystem, jarPath, destPath, false, hadoopConf)
    }

    if (!localJarFile.exists()) {
      // Verify copy succeeded
      throw new Exception(s"Did not see expected jar $jarFileName in $driverDir")
    }

    localJarFilename
  }

  /**
    *
    * @param builder
    * @param baseDir Driver 工作目录
    * @param supervise 失败是否重启
    */
  private def launchDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean) {
    builder.directory(baseDir) //Java API ：返回此进程生成器的工作目录。


    /**
      * 完成初始化工作，完成工作目录的stderr 和stdout的流重定向
      * @param process
      */
    def initialize(process: Process) = {
      // Redirect stdout and stderr to files
      val stdout = new File(baseDir, "stdout")
      //从定向输入流
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val header = "Launch Command: %s\n%s\n\n".format(
        builder.command.mkString("\"", "\" \"", "\""), "=" * 40)
      Files.append(header, stderr, UTF_8)
      //从定向输入流
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }


    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }//launchDriver结束

  private[deploy] def runCommandWithRetry(command: ProcessBuilderLike, initialize: Process => Unit,
                                          supervise: Boolean) {

    /**
      * Time to wait between submission retries.
      */
    var waitSeconds = 1
    /**
      * A run of this many seconds resets the exponential back-off.
      */
    val successfulRunDuration = 5

    /**
      * Driver失败是否重启？<br> var keepTrying = !killed <br>@volatile var killed 默认值为 false
      */
    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))

      synchronized {
        if (killed) {
          return
        }
        process = Some(command.start())
        //完成初始化工作，完成工作目录的stderr 和stdout的流重定向
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      val exitCode = process.get.waitFor() //导致当前线程等待，如有必要，一直要等到由该 Process 对象表示的进程已经终止。
      if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000) {
        waitSeconds = 1
      }

      /**
        * 如果supervise=true <br>exitCode是非正常退出<br> 没有被杀死  <br> 则会重启
        */
      if (supervise && exitCode != 0 && !killed) {
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }

      //
      keepTrying = supervise && exitCode != 0 && !killed
      finalExitCode = Some(exitCode)
    }
  }//runCommandWithRetry结束
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int)
}


/**
  * ProcessBuilderLike的伴生中完成了匿名内部类的创建
  */
private[deploy] trait ProcessBuilderLike {
  /**
    *  processBuilder.start()
    *  使用此进程生成器的属性启动一个新进程，
    * @return 一个新的 Process 对象，用于管理子进程
    */
  def start(): Process

  /**
    * 返回此进程生成器的操作系统程序和参数。
    * @return
    */
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {

  def apply(processBuilder: ProcessBuilder) = new ProcessBuilderLike {
    /**
      *  processBuilder.start()
      *  使用此进程生成器的属性启动一个新进程
      * @return 一个新的 Process 对象，用于管理子进程
      */
    def start() = processBuilder.start()

    /**
      * 返回此进程生成器的操作系统程序和参数。
      * @return
      */
    def command = processBuilder.command()
  }
}
