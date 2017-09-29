package com.rrdinsights.russell.spark.job

/**
  * An object for [[org.apache.spark.deploy.SparkSubmit]] options.
  *
  * Naming convention:
  * - UpperCamelCase the arguments directly passed to [[org.apache.spark.deploy.SparkSubmit]]
  * - Prepend "Conf" if the argument is passed as [[SparkOptions.Conf]]
  */
object SparkOptions {

  /**
    * Same as {{{ spark.app.name }}}
    */
  val AppName: String = "--name"

  val Master: String = "--master"

  val DefaultMaster: String = "yarn"

  val DeployMode: String = "--deploy-mode"

  val DeployModeClient: String = "client"

  val DeployModeCluster: String = "cluster"

  val DefaultDeployMode: String = DeployModeCluster

  val DriverMemory: String = "--driver-memory"

  val DefaultDriverMemory: String = "4G"

  /**
    * Same as {{{ spark.executor.memory }}}
    */
  val ExecutorMemory: String = "--executor-memory"

  val DefaultExecutorMemory: String = "4G"

  val Queue: String = "--queue"

  val DefaultQueue: String = "default"

  val ConfExecutorInstances: String = "spark.executor.instances"

  /**
    * Same as [[ConfExecutorInstances]]
    * Note that non-zero value turns {{{ spark.dynamicAllocation.enabled }}} off.
    */
  val NumExecutors: String = "--num-executors"

  /**
    * The default value for {{{ spark.executor.instances }}} per documentation.
    */
  val MinimumNumExecutors: Int = 2

  val Conf: String = "--conf"

  val ConfExecutorMemoryOverhead: String = "spark.yarn.executor.memoryOverhead"

  /**
    * The maximum number of attempts that will be made to submit the application.
    * http://spark.apache.org/docs/1.6.0/running-on-yarn.html
    */
  val ConfSparkYarnMaxAttempts: String = "spark.yarn.maxAppAttempts"

  /**
    * Note that this is Edge's custom argument added for convenience.
    *
    * Same as [[ConfExecutorMemoryOverhead]]
    * Added as [[Conf]] [[ConfExecutorMemoryOverhead]]=N where N is the memory overhead in MB.
    */
  val ExecutorMemoryOverhead: String = "--executor-memory-overhead"

  /**
    * Default as of Spark 1.6.0 is 200
    */
  val ConfSqlShufflePartitions: String = "spark.sql.shuffle.partitions"

  val ConfDriverExtraJavaOptions: String = "spark.driver.extraJavaOptions"

  val ConfExecutorExtraJavaOptions: String = "spark.executor.extraJavaOptions"

  val Files: String = "--files"

  /**
    * The name of the environment variable for setting additional Spark Configurations
    */
  val AdditionalSparkConfs: String = "SPARK_CONFS"
}
