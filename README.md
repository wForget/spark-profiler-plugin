# Spark JVM Profiler Plugin

## Fork Origin

This project is forked from the Apache Spark
[`connector/profiler`](https://github.com/apache/spark/tree/master/connector/profiler)
module, copied from the `master` branch at commit
[`c35e98fb487224d07a1051a35a78ef1f5efe2740`](https://github.com/apache/spark/tree/c35e98fb487224d07a1051a35a78ef1f5efe2740/connector/profiler).

The initial import preserves the upstream Scala source code, package names, and copyright
headers unchanged. The Maven configuration has been adapted for standalone builds, and
this document has been updated with build and runtime instructions. This project does
not require the Spark source checkout or inherit its parent POM and is not an official
Apache Spark release. The code is licensed under the Apache License 2.0; the upstream
license and notices are retained in [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Build

Requires JDK 17+ and Maven 3.6.3+. The default dependencies are Spark 4.2.0 and
Scala 2.13.18, with async-profiler loader 4.5-13 matching upstream master.

```bash
mvn clean verify
```

Output: `target/spark-profiler-plugin_2.13-0.1.0-SNAPSHOT.jar`.
`verify` compiles the source code, runs the tests, and packages the JAR.

Use `-Dspark.version=... -Dscala.version=...` to match your cluster versions.
The source code uses Spark internal APIs, so rebuild and verify compatibility whenever
you change versions. The Scala binary version is 2.13. The current upstream source
depends on `SparkContext.isDriver` and cannot compile against Spark 4.1.x without changes.

The generated JAR does not bundle dependencies. Spark and Scala are provided by the
cluster; supply `me.bechberger:ap-loader-all:4.5-13` at runtime, for example through
`--packages` as shown below. If the cluster cannot access Maven repositories, add a
previously downloaded loader JAR to `--jars` alongside the plugin JAR. Since the upstream
class names are preserved, avoid loading another Spark profiler module JAR at the same time.

## Executor Code Profiling

The spark profiler module enables code profiling of executors in cluster mode based on the [async profiler](https://github.com/async-profiler/async-profiler/blob/v4.0/README.md), a low overhead sampling profiler. This allows a Spark application to capture CPU and memory profiles for application running on a cluster which can later be analyzed for performance issues. The profiler captures [Java Flight Recorder (jfr)](https://access.redhat.com/documentation/es-es/red_hat_build_of_openjdk/17/html/using_jdk_flight_recorder_with_red_hat_build_of_openjdk/openjdk-flight-recorded-overview) files for each executor; these can be read by many tools including Java Mission Control and Intellij.

The profiler writes the jfr files to the executor's working directory in the executor's local file system and the files can grow to be large, so it is advisable that the executor machines have adequate storage. The profiler can be configured to copy the jfr files to a hdfs location before the executor shuts down.

Code profiling is currently only supported for

*   Linux (x64)
*   Linux (arm64)
*   Linux (musl, x64)
*   MacOS

To get maximum profiling information set the following jvm options for the executor :

```
spark.executor.extraJavaOptions=-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer
```

For more information on async_profiler see the [Async Profiler Manual](https://krzysztofslusarski.github.io/2022/12/12/async-manual.html)


To enable code profiling, first enable the code profiling plugin via

```
spark.plugins=org.apache.spark.profiler.ProfilerPlugin
```

Then enable the profiling in the configuration.

### Task spans

Set `spark.profiler.executor.taskSpan.enabled=true` to record each task attempt as a
`profiler.Span` event on profiled executors. Task spans are disabled by default and
require executor profiling to be enabled; executor sampling still applies.

Spans are written to the same JFR file as the profiling samples.
The span starts in the task start callback
and ends in the success or failure callback, including result serialization on
successful tasks. Task deserialization before the start callback is not included.

The span tag starts with `SparkTask`, followed by comma-separated `stageId`,
`stageAttemptNumber`, `partitionId`, `taskAttemptId`, `attemptNumber`, and `status`
(`succeeded` or `failed`) key-value pairs. These tags allow profiling
samples to be associated with individual task attempts, including retries and
speculative attempts. Spans are only emitted while a JFR recording is active.


### Task thread mappings in the Spark event log

Enable task thread events independently of JFR profiling and executor sampling:

```properties
spark.plugins=org.apache.spark.profiler.ProfilerPlugin
spark.eventLog.enabled=true
spark.profiler.executor.taskThreadEvent.enabled=true
```

The mapping option defaults to `false`. When both flags are enabled, each executor
reports one `org.apache.spark.profiler.SparkProfilerTaskThreadEvent` event after
its task success or failure callback. The driver writes it through Spark's event
logging listener. Besides Spark's required `Event` type discriminator, the event
contains only these six fields:

```json
{
  "Event": "org.apache.spark.profiler.SparkProfilerTaskThreadEvent",
  "taskId": 42,
  "executorId": "1",
  "javaThreadId": 73,
  "startTime": 1791792000000,
  "endTime": 1791792000123,
  "status": "succeeded"
}
```

`taskId` is `TaskContext.taskAttemptId()` (the same ID as Spark's `Task Info.Task ID`),
so retries and speculative attempts have distinct IDs. `javaThreadId` is the Java
thread ID within that executor, not an OS thread ID or a JFR thread reference.
`startTime` and `endTime` are executor wall-clock Unix timestamps in milliseconds,
covering the same task callbacks as task spans. `status` is `succeeded` or `failed`
(including tasks reported through the failure callback after cancellation).

Reporting is asynchronous and best effort. An executor that dies before reporting
completion may leave no mapping event. Install this plugin JAR on the History Server
classpath to deserialize these custom events during replay.

### Code profiling configuration

<table class="spark-config">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.profiler.driver.enabled</code></td>
  <td><code>false</code></td>
  <td>
    If true, turn on profiling in driver.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.executor.enabled</code></td>
  <td><code>false</code></td>
  <td>
    If true, turn on profiling in executors.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.executor.fraction</code></td>
  <td>0.10</td>
  <td>
    The fraction of executors on which to enable profiling. The executors to be profiled are picked at random.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.executor.taskSpan.enabled</code></td>
  <td><code>false</code></td>
  <td>
    If true, record task attempt spans in the JFR output of executors selected for profiling.
    Requires <code>spark.profiler.executor.enabled=true</code> and respects
    <code>spark.profiler.executor.fraction</code>.
  </td>
  <td>5.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.executor.taskThreadEvent.enabled</code></td>
  <td><code>false</code></td>
  <td>
    If true, record completed task-to-Java-thread mappings in the Spark event log.
    Requires <code>spark.eventLog.enabled=true</code>; independent of JFR profiling
    and executor sampling.
  </td>
  <td>5.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.dfsDir</code></td>
  <td>(none)</td>
  <td>
      An HDFS compatible path to which the profiler's output files are copied. The output files will be written as <i>dfsDir/{{APP_ID}}/profile-exec-{{EXECUTOR_ID}}.jfr</i> <br/>
      If no <i>dfsDir</i> is specified then the files are not copied over. Users should ensure there is sufficient disk space available otherwise it may lead to corrupt jfr files.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.localDir</code></td>
  <td><code>.</code> i.e. the executor's working dir</td>
  <td>
   The local directory in the executor container to write the jfr files to. If not specified the file will be written to the executor's working directory. Users should ensure there is sufficient disk space available on the system as running out of space may result in corrupt jfr file and even cause jobs to fail on systems like K8s.  
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.asyncProfiler.args</code></td>
  <td>event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s</td>
  <td>
      Arguments to pass to the Async Profiler. Detailed options are documented in the comments here:
      <a href="https://github.com/async-profiler/async-profiler/blob/v4.0/docs/ProfilerOptions.md">Profiler options</a>.  
       Note that the arguments to start, stop, specify output format, and output file do not have to be specified.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.dfsWriteInterval</code></td>
  <td>30</td>
  <td>
    Time interval, in seconds, after which the profiler output will be synced to DFS.
  </td>
  <td>4.0.0</td>
</tr>
</table>

### Kubernetes
On Kubernetes, spark will try to shut down the executor pods while the profiler files are still being saved. To prevent this set
```
  spark.kubernetes.executor.deleteOnTermination=false
```

### Example
```
./bin/spark-submit \
  --jars target/spark-profiler-plugin_2.13-0.1.0-SNAPSHOT.jar \
  --packages me.bechberger:ap-loader-all:4.5-13 \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  -c spark.executor.extraJavaOptions="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer" \
  -c spark.plugins=org.apache.spark.profiler.ProfilerPlugin \
  -c spark.profiler.executor.enabled=true \
  -c spark.profiler.executor.fraction=0.10 \
  -c spark.profiler.dfsDir=s3a://my-bucket/spark/profiles/ \
  -c spark.profiler.asyncProfiler.args=event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s \
  -c spark.kubernetes.executor.deleteOnTermination=false \
  <application-jar> \
  [application-arguments]
```
