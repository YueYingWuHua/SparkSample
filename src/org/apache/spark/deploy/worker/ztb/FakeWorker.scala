package org.apache.spark
package deploy
package worker
package ztb

import org.apache.spark.rpc.ThreadSafeRpcEndpoint
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.rpc.RpcEndpointRef
import scala.util.Success
import scala.util.Failure
import org.apache.spark.util.ThreadUtils
import scala.util.control.NonFatal
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.WorkerArguments
import org.apache.spark.util.Utils

class FakeWorker(
    override val rpcEnv: RpcEnv,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    endpointName: String,
    workDirPath: String = null,
    val conf: SparkConf,
    val securityMgr: SecurityManager)
    extends ThreadSafeRpcEndpoint {
  
  val host = rpcEnv.address.host
  val port = rpcEnv.address.port
  var count = masterRpcAddresses.length
  
  def connectWithMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.ask[MasterStateResponse](RequestMasterState)
      .onComplete {
        // This is a very fast action so we can use "ThreadUtils.sameThread"
        case Success(msg) =>
          msg.workers.foreach { x => println(x.coresFree) }
          count = count - 1
          if (count == 0) rpcEnv.shutdown
        case Failure(e) =>
          println(s"Cannot register with master: ${masterEndpoint.address}", e)
          System.exit(1)
      }(ThreadUtils.sameThread)
  }
  
  def getMasterStatus = {
     masterRpcAddresses.map { masterAddress =>
      new Runnable {
        override def run(): Unit = {
          try {
            println("Connecting to master " + masterAddress + "...")
            val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            connectWithMaster(masterEndpoint)
            rpcEnv.stop(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => println(s"Failed to connect to master $masterAddress", e)
          }
        }
      }.run
    }
    println("get master status done")
  }
  
  override def onStart() = {
    println("starting fakeWorker")
    getMasterStatus
    println("closing fakeWorker")
  }
  
}

object FakeWorker {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "fakeWorker"
  
  def main(argString: Array[String]): Unit = {
    val rpc = startWorker(argString)
    rpc.awaitTermination()
  }
  
  def startWorker(argString: Array[String]) = {
    val conf = new SparkConf
    //val args = new WorkerArguments(argstring, conf)
    //val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores, args.memory, args.masters, args.workDir, conf = conf)
    val rpcEnv = startRpcEnvAndEndpoint("192.168.12.102", 6666, 4321, 1, 2, 
        Utils.parseStandaloneMasterUrls("spark://192.168.12.147:7077"), "D:/workerdir", conf = conf)
    rpcEnv
  }
  
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new FakeWorker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, ENDPOINT_NAME, workDir, conf, securityMgr))
    rpcEnv
  }
}