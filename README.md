# README #

Sparklens is a profiling tool for Spark with built-in Spark Scheduler simulator. Its primary goal is to make it easy 
to understand the scalability limits of spark applications. It helps in understanding how efficiently is a given 
spark application using the compute resources provided to it. May be your application will run faster with more 
executors and may be it wont. Sparklens can answer this question by looking at a single run of your application. 

It helps you narrow down to few stages (or driver, or skew or lack of tasks) which are limiting your application 
from scaling out and provides contextual information about what could be going wrong with these stages. Primarily 
it helps you approach spark application tuning as a well defined method/process instead of something you learn by 
trial and error, saving both developer and compute time. 

## Adaptations to sparklens_1.0

We have adapted sparklens so that it can push it's metrics to a push-gateway for prometheus to collect, so that they can
then be displayed using Grafana. It also sends a text file with the "per stage" metrics to
https://s3.console.aws.amazon.com/s3/buckets/hs-spark-lens-reports/(job-name)/stageAnalyser.txt.

## How to implement?

Add 3 extra spark configurations-
--conf spark.homestead.sparklens.batchId=xxx
--conf spark.homestead.sparklens.job.name=(eg. modelbuilder/esloader/ aggregationLoader)
--conf spark.homestead.sparklens.pushgateway.host=(prod = a519f9203fed811e88f05024520558bb-1364591803.us-east-1.elb.amazonaws.com) (dev = a9bc920a5158911e984e00a96588aa2c-1925493531.us-east-1.elb.amazonaws.com)

Add an extra --driver-class-path as the sparklens jar which can be found at homestead-spark-monitor\hs-sparklens_1.0\target\scala-2.11\hs-sparklens-assembly-0.13.17.jar
Add an extra spark listener --conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
Add the jar hs-sparklens-assembly-0.13.17.jar to the list of jars or files

## What does it report?

The main benefit of our sparklens reporter is it's ability to estimate completion time and cluster utilisation based on
different numbers of executors being used. This allows the user to see if increasing or decreasing the executor count
would be beneficial. This estimation can be made with a single run of the spark job.

An example display of the per stage metrics is below e.g. Input, Output, Shuffle Input and Shuffle Output per stage.
**OneCoreComputeHours** available and used per stage to find out inefficient stages.

```
Total tasks in all stages 189446
Per Stage  Utilization
Stage-ID   Wall    Task      Task     IO%    Input     Output    ----Shuffle-----    -WallClockTime-    --OneCoreComputeHours---   MaxTaskMem
          Clock%  Runtime%   Count                               Input  |  Output    Measured | Ideal   Available| Used%|Wasted%                                  
       0    0.00    0.00         2    0.0  254.5 KB    0.0 KB    0.0 KB    0.0 KB    00m 04s   00m 00s    05h 21m    0.0  100.0    0.0 KB 
       1    0.00    0.01        10    0.0  631.1 MB    0.0 KB    0.0 KB    0.0 KB    00m 07s   00m 00s    08h 18m    0.2   99.8    0.0 KB 
       2    0.00    0.40      1098    0.0    2.1 GB    0.0 KB    0.0 KB    5.7 GB    00m 14s   00m 00s    16h 25m    3.2   96.8    0.0 KB 
       3    0.00    0.09       200    0.0    0.0 KB    0.0 KB    5.7 GB    2.3 GB    00m 03s   00m 00s    04h 35m    2.6   97.4    0.0 KB 
       4    0.00    0.03       200    0.0    0.0 KB    0.0 KB    2.3 GB    0.0 KB    00m 01s   00m 00s    01h 13m    2.9   97.1    0.0 KB 
       7    0.00    0.03       200    0.0    0.0 KB    0.0 KB    2.3 GB    2.7 GB    00m 02s   00m 00s    02h 27m    1.7   98.3    0.0 KB 
       8    0.00    0.03        38    0.0    0.0 KB    0.0 KB    2.7 GB    2.7 GB    00m 05s   00m 00s    06h 20m    0.6   99.4    0.0 KB 
```

There are also the following metrics available (as a minimum, maximum, sum and mean):

diskBytesSpilled  - The number of on-disk bytes spilled by this task.
executorRuntime - Time the executor spends actually running the task (including fetching shuffle data).
inputBytesRead - Total number of bytes read.
jvmGCTime - Amount of time the JVM spent in garbage collection while executing this task.
memoryBytesSpilled - The number of in-memory bytes spilled by this task.
outputBytesWritten - Total number of bytes written.
peakExecutionMemory - Peak memory used by internal data structures created during shuffles, aggregations and
                      joins. The value of this accumulator should be approximately the sum of the peak sizes
                      across all such data structures created in this task. For SQL jobs, this only tracks all
                      unsafe operators and ExternalSort.
resultSize - The number of bytes this task transmitted back to the driver as the TaskResult.
shuffleReadBytesRead - Total bytes fetched in the shuffle by this task (both remote and local).
shuffleReadFetchWaitTime - Time the task spent waiting for remote shuffle blocks. This only includes the time
                           blocking on shuffle input data. For instance if block B is being fetched while the task is
                           still not finished processing block A, it is not considered to be blocking on block B.
shuffleReadLocalBlocks - Number of local blocks fetched in this shuffle by this task.
shuffleReadRecordsRead - Total number of records read from the shuffle by this task.
shuffleReadRemoteBlocks - Number of remote blocks fetched in this shuffle by this task.
shuffleWriteBytesWritten - Number of bytes written for the shuffle by this task.
shuffleWriteRecordsWritten - Total number of records written to the shuffle by this task.
shuffleWriteTime - Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
taskDuration


## How to set up a sparklens Grafana dashboard

1. Create new dashboard
2. Click on dashboard settings (top right hand corner)
3. Select variables from the left hand side
4. Add a new variable
    Name: batch_id
    Label: Batch ID
    Type: Query
    Data Source: Prom1
    Query: label_values(batch_id)
    Sort: Numerical(desc)
    Multivalue: Yes
    Include all option: Yes
    Custom all value: .*
5. Add a query
    Change "Queries to:" from default to Prom1
    Add details to your query in the following format - Metric_name{job="xxx", batch_id=~"$batch_id"} - metric names are below
    Down the left hand side click on visualisation and choose between gauge, graph etc.
    Edit variables as you please
    Down the left hand side, click on general and add a title
Hint: If doing similar queries (based on different executor counts for example), once you have written one, you can copy and paste the query editing only what you need to

Metric names for queries:
    Cluster_Utilization_with_Xpercent_Executors{}
    Time_With_Xpercent_Executors{}
    oneCoreComputeHours_mins{}
    oneCoreComputeHours_wasted_by_driver_mins{}
    memoryBytesSpilled_max_mb{}
    executor_oneCoreComputeHours_available_mins{}
    executor_oneCoreComputeHours_used_mins{}
    time_spent_in_driver{}
    time_spent_in_executor{}
    minimum_app_time_critical_path{}
    minimum_time_with_same_executors{}
    total_executors{}
    maximum_concurrent_executors{}
    total_hosts{}
    maximum_concurrent_hosts{}
Aggregate metric names for queries (replace x with min, max, sum or mean)
    diskBytesSpilled_x_mb{}
    shuffleWriteTime_x_secs{}
    shuffleWriteBytesWritten_x_mb{}
    shuffleWriteRecordsWritten_x_mb{}
    shuffleReadFetchWaitTime_x_secs{}
    shuffleReadBytesRead_x_mb{}
    shuffleReadRecordsRead_x{}
    shuffleReadLocalBlocks_x{}
    shuffleReadRemoteBlocks_x{}
    executorRuntime_x_secs{}
    jvmGCTime_x_secs{}
    executorCpuTime_x_secs{}
    resultSize_x{}
    inputBytesRead_x_mb{}
    outputBytesWritten_x_mb{}
    memoryBytesSpilled_x_mb{}
    diskBytesSpilled_x_mb{}
    peakExecutionMemory_x_mb{}
    taskDuration_x_secs{}

