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

package com.qubole.sparklens

import java.net.URI

import com.qubole.sparklens.analyzer._
import com.qubole.sparklens.common.{AggregateMetrics, AggregateValue, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by rohitk on 21/09/17.
  *
  *
  */

class QuboleJobListener(sparkConf: SparkConf)  extends SparkListener {

  protected val appInfo          = new ApplicationInfo()
  protected val executorMap      = new mutable.HashMap[String, ExecutorTimeSpan]()
  protected val hostMap          = new mutable.HashMap[String, HostTimeSpan]()
  protected val jobMap           = new mutable.HashMap[Long, JobTimeSpan]
  protected val jobSQLExecIDMap  = new mutable.HashMap[Long, Long]
  protected val stageMap         = new mutable.HashMap[Int, StageTimeSpan]
  protected val stageIDToJobID   = new mutable.HashMap[Int, Long]
  protected val failedStages     = new ListBuffer[String]
  protected val appMetrics       = new AggregateMetrics()
  val registry                   = new CollectorRegistry()
  val lbl                        = sparkConf.get("spark.homestead.sparklens.batchId","unknown")
  val job                        = sparkConf.get("spark.homestead.sparklens.job.name","unknown")


  private def hostCount():Int = hostMap.size

  private def executorCount(): Int = executorMap.size

  private def coresPerExecutor(): Int = {
    // just for the fun
    executorMap.values.map(x => x.cores).sum/executorMap.size
  }
  private def executorsForHost(hostID: String) : Seq[String] = {
    executorMap.values.filter(_.hostID.equals(hostID)).map(x => x.executorID).toSeq
  }
  private def executorsPerHost(): Double = {
    executorCount()/hostCount()
  }
  private def appAggregateMetrics(): AggregateMetrics = appMetrics

  private def hostAggregateMetrics(hostID: String): Option[AggregateMetrics] = {
    hostMap.get(hostID).map(x => x.hostMetrics)
  }

  private def executorAggregateMetrics(executorID: String): Option[AggregateMetrics] = {
    executorMap.get(executorID).map(x => x.executorMetrics)
  }

  private def driverTimePercentage(): Option[Double] = {
    if (appInfo.endTime == 0) {
      return None
    }
    val totalTime = appInfo.endTime - appInfo.startTime
    val jobTime   = jobMap.values.map(x => (x.endTime - x.startTime)).sum
    Some((totalTime-jobTime)/totalTime)
  }

  private def jobTimePercentage(): Option[Double] = {
    if (appInfo.endTime == 0) {
      return None
    }
    val totalTime = appInfo.endTime - appInfo.startTime
    val jobTime   = jobMap.values.map(x => (x.endTime - x.startTime)).sum
    Some(jobTime/totalTime)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskMetrics = taskEnd.taskMetrics
    val taskInfo    = taskEnd.taskInfo

    if (taskMetrics == null) return
    
    //update app metrics
    appMetrics.update(taskMetrics, taskInfo)

    val executorTimeSpan = executorMap.get(taskInfo.executorId)
    if (executorTimeSpan.isDefined) {
      //update the executor metrics
      executorTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
    }
    val hostTimeSpan = hostMap.get(taskInfo.host)
    if (hostTimeSpan.isDefined) {
      //also update the host metrics
      hostTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
    }

    val stageTimeSpan = stageMap.get(taskEnd.stageId)
    if (stageTimeSpan.isDefined) {
      //update stage metrics
      stageTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
      stageTimeSpan.get.updateTasks(taskInfo, taskMetrics)
    }
    val jobID = stageIDToJobID.get(taskEnd.stageId)
    if (jobID.isDefined) {
      val jobTimeSpan = jobMap.get(jobID.get)
      if (jobTimeSpan.isDefined) {
        //update job metrics
        jobTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
      }
    }

    if (taskEnd.taskInfo.failed) {
      //println(s"\nTask Failed \n ${taskEnd.reason}"
    }
  }



  private[this] def dumpData(appContext: AppContext): Unit = {
    val dumpDir = getDumpDirectory(sparkConf)
    println(s"Saving sparkLens data to ${dumpDir}")
    val fs = FileSystem.get(new URI(dumpDir), new Configuration())
    val stream = fs.create(new Path(s"${dumpDir}/${appInfo.applicationID}.sparklens.json"))
    val jsonString = appContext.toString
    stream.writeBytes(jsonString)
    stream.close()
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    //println(s"Application ${applicationStart.appId} started at ${applicationStart.time}")
    appInfo.applicationID = applicationStart.appId.getOrElse("NA")
    appInfo.startTime     = applicationStart.time
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    //println(s"Application ${appInfo.applicationID} ended at ${applicationEnd.time}")
    appInfo.endTime = applicationEnd.time

    val appContext = new AppContext(appInfo,
      appMetrics,
      hostMap,
      executorMap,
      jobMap,
      jobSQLExecIDMap,
      stageMap,
      stageIDToJobID)

    asyncReportingEnabled(sparkConf) match {
      case true => {
        println("Reporting disabled. Will save sparklens data file for later use.")
        dumpData(appContext)
      }
      case false => {
        if (dumpDataEnabled(sparkConf)) dumpData(appContext)
        val metrics = AppAnalyzer.startAnalyzers(appContext)
        metrics foreach(x => createPromMetric(x._2, x._1))
      }
    }
    aggregateMetricsToPrometheus()
  }
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val executorTimeSpan = executorMap.get(executorAdded.executorId)
    if (!executorTimeSpan.isDefined) {
      val timeSpan = new ExecutorTimeSpan(executorAdded.executorId,
        executorAdded.executorInfo.executorHost,
        executorAdded.executorInfo.totalCores)
      timeSpan.setStartTime(executorAdded.time)
       executorMap(executorAdded.executorId) = timeSpan
    }
    val hostTimeSpan = hostMap.get(executorAdded.executorInfo.executorHost)
    if (!hostTimeSpan.isDefined) {
      val executorHostTimeSpan = new HostTimeSpan(executorAdded.executorInfo.executorHost)
      executorHostTimeSpan.setStartTime(executorAdded.time)
      hostMap(executorAdded.executorInfo.executorHost) = executorHostTimeSpan
    }
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val executorTimeSpan = executorMap(executorRemoved.executorId)
    executorTimeSpan.setEndTime(executorRemoved.time)
    //We don't get any event for host. Will not try to check when the hosts go out of service
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val jobTimeSpan = new JobTimeSpan(jobStart.jobId)
    jobTimeSpan.setStartTime(jobStart.time)
    jobMap(jobStart.jobId) = jobTimeSpan
    jobStart.stageIds.foreach( stageID => {
      stageIDToJobID(stageID) = jobStart.jobId
    })
    val sqlExecutionID = jobStart.properties.getProperty("spark.sql.execution.id")
    if (sqlExecutionID != null && !sqlExecutionID.isEmpty) {
      jobSQLExecIDMap(jobStart.jobId) = sqlExecutionID.toLong
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobTimeSpan = jobMap(jobEnd.jobId)
    jobTimeSpan.setEndTime(jobEnd.time)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (!stageMap.get(stageSubmitted.stageInfo.stageId).isDefined) {
      val stageTimeSpan = new StageTimeSpan(stageSubmitted.stageInfo.stageId,
        stageSubmitted.stageInfo.numTasks)
      stageTimeSpan.setParentStageIDs(stageSubmitted.stageInfo.parentIds)
      if (stageSubmitted.stageInfo.submissionTime.isDefined) {
        stageTimeSpan.setStartTime(stageSubmitted.stageInfo.submissionTime.get)
      }
      stageMap(stageSubmitted.stageInfo.stageId) = stageTimeSpan
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageTimeSpan = stageMap(stageCompleted.stageInfo.stageId)
    if (stageCompleted.stageInfo.completionTime.isDefined) {
      stageTimeSpan.setEndTime(stageCompleted.stageInfo.completionTime.get)
    }
    if (stageCompleted.stageInfo.submissionTime.isDefined) {
      stageTimeSpan.setStartTime(stageCompleted.stageInfo.submissionTime.get)
    }

    if (stageCompleted.stageInfo.failureReason.isDefined) {
      //stage failed
      val si = stageCompleted.stageInfo
      failedStages += s""" Stage ${si.stageId} attempt ${si.attemptId} in job ${stageIDToJobID(si.stageId)} failed.
                      Stage tasks: ${si.numTasks}
                      """
      stageTimeSpan.finalUpdate()
    }else {
      val jobID = stageIDToJobID(stageCompleted.stageInfo.stageId)
      val jobTimeSpan = jobMap(jobID)
      jobTimeSpan.addStage(stageTimeSpan)
      stageTimeSpan.finalUpdate()
    }
  }

  def setGaugeMs(metric: AggregateMetrics.Metric): Unit = {
    val meanMetric: Gauge = Gauge.build().name(metric.toString + "_mean_secs").help("Mean (s)").labelNames("batch_id").register(registry)
    val maxMetric: Gauge = Gauge.build().name(metric.toString + "_max_secs").help("Maximum (s)").labelNames("batch_id").register(registry)
    val minMetric: Gauge = Gauge.build().name(metric + "_min_secs").help("Minimum (s)").labelNames("batch_id").register(registry)
    val sumMetric: Gauge = Gauge.build().name(metric + "_sum_secs").help("Sum (s)").labelNames("batch_id").register(registry)
    minMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).min)/1000).toDouble)
    meanMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).mean))/1000)
    maxMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).max)/1000).toDouble)
    sumMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).value)/1000).toDouble)
  }

  def setGaugeNs(metric: AggregateMetrics.Metric): Unit = {
    val meanMetric: Gauge = Gauge.build().name(metric.toString + "_mean_secs").help("Mean (s)").labelNames("batch_id").register(registry)
    val maxMetric: Gauge = Gauge.build().name(metric.toString + "_max_secs").help("Maximum (s)").labelNames("batch_id").register(registry)
    val minMetric: Gauge = Gauge.build().name(metric + "_min_secs").help("Minimum (s)").labelNames("batch_id").register(registry)
    val sumMetric: Gauge = Gauge.build().name(metric + "_sum_secs").help("Sum (s)").labelNames("batch_id").register(registry)
    minMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).min)/1000000).toDouble)
    meanMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).mean))/1000000)
    maxMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).max)/1000000).toDouble)
    sumMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).value)/1000000).toDouble)
  }

  def setGaugeBytes(metric: AggregateMetrics.Metric): Unit = {
    val meanMetric: Gauge = Gauge.build().name(metric.toString + "_mean_mb").help("Mean (mb)").labelNames("batch_id").register(registry)
    val maxMetric: Gauge = Gauge.build().name(metric.toString + "_max_mb").help("Maximum (mb)").labelNames("batch_id").register(registry)
    val minMetric: Gauge = Gauge.build().name(metric + "_min_mb").help("Minimum (mb)").labelNames("batch_id").register(registry)
    val sumMetric: Gauge = Gauge.build().name(metric + "_sum_mb").help("Sum (mb)").labelNames("batch_id").register(registry)
    minMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).min)/1024/1024).toDouble)
    meanMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).mean))/1024/1024)
    maxMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).max)/1024/1024).toDouble)
    sumMetric.labels(lbl).set(((appMetrics.map.getOrElse(metric, new AggregateValue).value)/1024/1024).toDouble)
  }

  def setGauges(metric: AggregateMetrics.Metric): Unit = {
    val meanMetric: Gauge = Gauge.build().name(metric.toString + "_mean").help("Mean of values.").labelNames("batch_id").register(registry)
    val maxMetric: Gauge = Gauge.build().name(metric.toString + "_max").help("Maximum value.").labelNames("batch_id").register(registry)
    val minMetric: Gauge = Gauge.build().name(metric + "_min").help("Minimum value.").labelNames("batch_id").register(registry)
    val sumMetric: Gauge = Gauge.build().name(metric + "_sum").help("Minimum value.").labelNames("batch_id").register(registry)
    minMetric.labels(lbl).set((appMetrics.map.getOrElse(metric, new AggregateValue).min).toDouble)
    meanMetric.labels(lbl).set((appMetrics.map.getOrElse(metric, new AggregateValue).mean))
    maxMetric.labels(lbl).set((appMetrics.map.getOrElse(metric, new AggregateValue).max).toDouble)
    sumMetric.labels(lbl).set((appMetrics.map.getOrElse(metric, new AggregateValue).value).toDouble)
  }
  def aggregateMetricsToPrometheus(): Unit = {
    setGaugeBytes(AggregateMetrics.diskBytesSpilled)
    setGaugeMs(AggregateMetrics.executorRuntime)
    setGaugeBytes(AggregateMetrics.inputBytesRead)
    setGaugeMs(AggregateMetrics.jvmGCTime)
    setGaugeBytes(AggregateMetrics.memoryBytesSpilled)
    setGaugeBytes(AggregateMetrics.shuffleWriteBytesWritten)
    setGaugeBytes(AggregateMetrics.peakExecutionMemory)
    setGaugeBytes(AggregateMetrics.resultSize)
    setGaugeBytes(AggregateMetrics.shuffleReadBytesRead)
    setGaugeMs(AggregateMetrics.shuffleReadFetchWaitTime)
    setGaugeBytes(AggregateMetrics.shuffleReadLocalBlocks)
    setGauges(AggregateMetrics.shuffleReadRecordsRead)
    setGauges(AggregateMetrics.shuffleReadRemoteBlocks)
    setGauges(AggregateMetrics.shuffleWriteRecordsWritten)
    setGaugeNs(AggregateMetrics.shuffleWriteTime)
    setGaugeMs(AggregateMetrics.taskDuration)

    val pg = sparkConf.get("spark.homestead.sparklens.pushgateway.host","localhost")
    val pushGateway = new PushGateway(pg + ":9091")
    pushGateway.pushAdd(registry, job)

  }

  def createPromMetric(number: Double, name: String): Unit = {
    val lbl = sparkConf.get("spark.homestead.sparklens.batchId","unknown")
    val job = sparkConf.get("spark.homestead.sparklens.job.name","unknown")
    val pg = sparkConf.get("spark.homestead.sparklens.pushgateway.host","localhost")
    val pushGateway = new PushGateway(pg + ":9091")

    try {
      val Metric: Gauge = Gauge.build().name(name).labelNames("batch_id").help("Check gauge name").register(registry)
      Metric.labels(lbl).set(number)
    }
    finally {
      pushGateway.pushAdd(registry, job) }
  }

}