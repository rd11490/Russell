package com.rrdinsights.russell.spark.job

import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait ClusterJobRunner extends JobRunner with Tool {

  override val sparkConf: SparkConf = new SparkConf() // TODO adjust to fit AWS needs
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.driver.host", "localhost")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", "app")
    .set("spark.default.parallelism", "1")
    .set("spark.shuffle.compress", "false")
    .set("spark.broadcast.compress", "false")
    .set("spark.rdd.compress", "false")

  /** Entry point to the job execution. The default implementation simply calls `runSpark2` and then returns an
    * `ExitStatus.Success` code assuming that any errors would be rather signaled as exceptions.
    *
    * @inheritdoc
    */
  override def run(strings: Array[String]): Int = {
    runSpark2(strings)(spark)
    ExitStatus.Success.status
  }

  /** Override if the job need custom configuration.
    *
    * @param args the arguments (typically commandline arguments) needed for setting [[SparkJobConfiguration]]
    * @return an instance of [[SparkJobConfiguration]]
    */
  def jobConfiguration(args: Array[String] = Array.empty): SparkJobConfiguration = new SparkJobConfiguration()
    // Work around https://issues.apache.org/jira/browse/SPARK-17511
    .withConf("spark.yarn.max.executor.failures", "10")

  def main(args: Array[String]): Unit = {
    JobRunner.validateExitCode(ToolRunner.run(this, args))
  }
}