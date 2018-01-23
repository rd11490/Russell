package com.rrdinsights.russell.spark.job

import SparkOptions._
import com.rrdinsights.russell.utils.NullSafe

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

/**
  * Stripped-down version of [[org.apache.spark.deploy.SparkSubmitArguments]]
  */
final class SparkJobConfiguration {

  /**
    * [[AppName]] aka {{{ spark.app.name }}}
    * [[None]] by default
    * Note that intentionally not exposing publicly to prevent users from modifying this directly.
    */
  private[this] var _appName: Option[String] = None

  /**
    * [[Conf]] Property=Value
    */
  private[this] val _configurations: MMap[String, String] = MMap.empty

  /**
    * [[DriverMemory]]
    */
  private[this] var _driverMemory: String = DefaultDriverMemory

  /**
    * [[ExecutorMemory]] aka {{{ spark.executor.memory }}}
    */
  private[this] var _executorMemory: String = DefaultExecutorMemory

  /**
    * [[Queue]]
    */
  private[this] var queue: String = DefaultQueue


  /**
    * [[NumExecutors]] aka {{{ spark.executor.memory }}}
    * Note that [[None]] means "turn {{{ spark.dynamicAllocation.enabled }}} on"
    */
  private var _numExecutors: Option[Int] = None

  /**
    * [[Master]]
    */
  var master: String = DefaultMaster

  /**
    * [[DeployMode]]
    */
  var deployMode: String = DefaultDeployMode

  private[this] val _driverJavaOptions: ArrayBuffer[String] = new ArrayBuffer()

  private[this] val _executorJavaOptions: ArrayBuffer[String] = new ArrayBuffer()

  private[spark] val files: ArrayBuffer[String] = new ArrayBuffer()

  def withMaster(masterName: String): this.type = {
    master = masterName
    this
  }

  def withDeployMode(mode: String): this.type = {
    deployMode = mode
    this
  }

  def withAppName(name: String): this.type = {
    if (NullSafe.isNotNullOrEmpty(name)) {
      throw new IllegalArgumentException("user-specified AppName cannot be null or empty!")
    }
    _appName = Some(name)
    this
  }

  private def withAppNameOpt(nameOpt: Option[String]): this.type = {
    _appName = nameOpt
    this
  }

  /**
    * @return [[Some]] if the AppName is set or [[None]]
    */
  def appName: Option[String] = _appName

  /**
    * Any conf one would pass in as [[Conf]] PROP=VALUE
    *
    * @param property the name of the property
    * @param value    the value to set
    * @return this instance with the property added
    */
  def withConf(property: String, value: String): this.type = {
    _configurations += property -> value
    this
  }

  def withConfs(confs: Map[String, String]): this.type = {
    _configurations ++= confs
    this
  }

  def configurations: Map[String, String] = _configurations.toMap

  def withDriverMemoryGB(gb: Int): this.type = withDriverMemory(gb + "G")

  def withDriverMemoryMB(mb: Int): this.type = withDriverMemory(mb + "M")

  /**
    * Intentionally not exposing to prevent users from appending M or G.
    *
    * @param mem the memory overhead in terms of MB or GB
    * @return this instance with driver memory set
    */
  private def withDriverMemory(mem: String): this.type = {
    _driverMemory = mem
    this
  }

  def driverMemory: String = _driverMemory

  def withExecutorMemoryGB(gb: Int): this.type = withExecutorMemory(gb + "G")

  def withExecutorMemoryMB(mb: Int): this.type = withExecutorMemory(mb + "M")

  /**
    * Intentionally not exposing to prevent users from appending M or G.
    *
    * @param mem the memory overhead in terms of MB or GB
    * @return this instance with executor memory set
    */
  private def withExecutorMemory(mem: String): this.type = {
    _executorMemory = mem
    this
  }

  def executorMemory: String = _executorMemory

  /**
    * @param mb the memory overhead in MB
    * @return this instance with memory overhead set
    */
  def withExecutorMemoryOverheadMB(mb: Int): this.type = withExecutorMemoryOverhead(mb.toString)

  /**
    * Intentionally not exposing to prevent users from appending M or G.
    *
    * @param mem the memory overhead in terms of MB
    * @return this instance with memory overhead set
    */
  private def withExecutorMemoryOverhead(mem: String): this.type = {
    _configurations += ConfExecutorMemoryOverhead -> mem
    this
  }

  /**
    * If [[None]] is returned then the default (executorMemory * 0.10, with minimum of 384) will be used.
    *
    * @return the executor memory overhead in terms of MB or [[None]] if not specified
    */
  def executorMemoryOverheadOpt: Option[Int] =
  _configurations.get(ConfExecutorMemoryOverhead).map(_.toInt)

  def withQueue(queueName: String): this.type = {
    queue = queueName
    this
  }

  /**
    * Note that specifying [[NumExecutors]] >= [[MinimumNumExecutors]] turns {{{ spark.dynamicAllocation.enabled }}} off.
    * If num < [[MinimumNumExecutors]], it is ignored.
    *
    * @param num the number of executors
    * @return this instance with [[NumExecutors]] specified
    */
  def withNumExecutors(num: Int): this.type = {
    if (num >= MinimumNumExecutors) {
      _numExecutors = Some(num)
    }
    this
  }

  private def withNumExecutorsOpt(numOpt: Option[Int]): this.type = {
    _numExecutors = numOpt
    this
  }

  /**
    * @return [[Some]] if specified number of executors is greater than or equal to [[MinimumNumExecutors]] else [[None]]
    */
  def numExecutors: Option[Int] = _numExecutors

  /**
    * Appends to both [[ConfDriverExtraJavaOptions]] and [[ConfExecutorExtraJavaOptions]]
    *
    * @param opt a Java option to append
    * @return this instance with the Java option appended to [[ConfDriverExtraJavaOptions]] and [[ConfExecutorExtraJavaOptions]]
    */
  def withExtraJavaOption(opt: String): this.type =
  withDriverExtraJavaOption(opt)
    .withExecutorExtraJavaOption(opt)

  /**
    * Appends to both [[ConfDriverExtraJavaOptions]] and [[ConfExecutorExtraJavaOptions]]
    *
    * @param options a sequence of Java option to append
    * @return this instance with the Java options appended to [[ConfDriverExtraJavaOptions]] and [[ConfExecutorExtraJavaOptions]]
    */
  def withExtraJavaOptions(options: Seq[String]): this.type = {
    options.foreach(withExtraJavaOption)
    this
  }

  /**
    * Appends to [[ConfDriverExtraJavaOptions]]
    *
    * @param opt a Java option to append
    * @return this instance with the Java option appended to [[ConfDriverExtraJavaOptions]]
    */
  def withDriverExtraJavaOption(opt: String): this.type = {
    _driverJavaOptions += opt
    this
  }

  /**
    * Appends to both [[ConfDriverExtraJavaOptions]]
    *
    * @param options a sequence of Java option to append
    * @return this instance with the Java options appended to [[ConfDriverExtraJavaOptions]]
    */
  def withDriverExtraJavaOptions(options: Seq[String]): this.type = {
    options.foreach(withDriverExtraJavaOption)
    this
  }

  def driverExtraJavaOption: Seq[String] = _driverJavaOptions

  /**
    * Appends to [[ConfExecutorExtraJavaOptions]]
    *
    * @param opt a Java option to appended
    * @return this instance with the Java option appended to [[ConfExecutorExtraJavaOptions]]
    */
  def withExecutorExtraJavaOption(opt: String): this.type = {
    _executorJavaOptions += opt
    this
  }

  /**
    * Appends to both [[ConfExecutorExtraJavaOptions]]
    *
    * @param options a sequence of Java option to append
    * @return this instance with the Java options appended to [[ConfExecutorExtraJavaOptions]]
    */
  def withExecutorExtraJavaOptions(options: Seq[String]): this.type = {
    options.foreach(withExecutorExtraJavaOption)
    this
  }

  def executorExtraJavaOption: Seq[String] = _executorJavaOptions

  def withFile(opt: String): this.type = {
    files += opt
    this
  }

  def build: Seq[String] = {
    addDefaultConfs()
    addExtraConfsFromEnv()
    Seq(Master, master, DeployMode, deployMode) ++
      _appName.map(Seq(AppName, _)).getOrElse {
        Seq.empty
      } ++
      Seq(
        Queue, queue,
        DriverMemory, driverMemory,
        ExecutorMemory, executorMemory) ++
      _numExecutors.map(n => Seq(NumExecutors, n.toString)).getOrElse {
        Seq.empty
      } ++
      (if (files.nonEmpty) Seq(SparkOptions.Files, files.mkString(",")) else Nil) ++
      buildConfs
  }

  private[this] def buildConfs: Seq[String] =
    _configurations.map(kv => s"${kv._1}=${kv._2}")
      .toSeq.sorted // sort in lexicographical order BEFORE flatMapping (mainly for unit-testability)
      .flatMap(c => Seq(Conf, c))

  private[this] def addExtraConfsFromEnv(): Unit = SparkJobConfiguration.buildWithArgs(this, extraConfs())

  private[this] def extraConfs(): Array[String] = sys.env.get(SparkOptions.AdditionalSparkConfs)
    .map(_.split("\\s+").filterNot(NullSafe.isNotNullOrEmpty))
    .getOrElse {
      Array.empty
    }

  private[this] def addDefaultConfs(): Unit = withConf(SparkOptions.ConfSparkYarnMaxAttempts, "1")


  override def toString: String = build.mkString("SparkConfigurations[", " ", "]")
}

object SparkJobConfiguration {

  /**
    * Assumes that args(0) is the class name.
    *
    * @param args the commandline arguments
    * @return a SparkConfiguration instance
    */
  def apply(args: Array[String]): SparkJobConfiguration =
  buildWithArgs(new SparkJobConfiguration(), args)

  /**
    * Copy-constructs with the pre-configured [[SparkJobConfiguration]]
    *
    * @param preConfigured a preset [[SparkJobConfiguration]] instance
    * @param args          additional arguments coming from commandline
    * @return the new [[SparkJobConfiguration]] with all configurations in preConfigured
    */
  def apply(preConfigured: SparkJobConfiguration, args: Array[String] = Array.empty): SparkJobConfiguration =
  buildWithArgs(
    new SparkJobConfiguration()
      .withMaster(preConfigured.master)
      .withDeployMode(preConfigured.deployMode)
      .withAppNameOpt(preConfigured.appName)
      .withDriverMemory(preConfigured.driverMemory)
      .withExecutorMemory(preConfigured.executorMemory)
      .withNumExecutorsOpt(preConfigured._numExecutors)
      .withConfs(preConfigured.configurations)
      .withDriverExtraJavaOptions(preConfigured.driverExtraJavaOption)
      .withExecutorExtraJavaOptions(preConfigured.executorExtraJavaOption),
    args)

  /**
    * @param conf pre-configured [[SparkJobConfiguration]]
    * @param args supersedes pre-configured [[SparkJobConfiguration]]
    * @return a [[SparkJobConfiguration]] instance with both pre-configuration and additional configurations combined
    */
  private def buildWithArgs(conf: SparkJobConfiguration, args: Array[String]): SparkJobConfiguration = {
    if (args.length % 2 == 0) {
      args.grouped(2).map(arg => (arg(0), arg(1))).foreach {
        case (Master, master) => conf.withMaster(master)
        case (DeployMode, mode) => conf.withDeployMode(mode)
        case (AppName, name) => conf.withAppName(name)
        case (DriverMemory, dm) => conf.withDriverMemory(dm)
        case (ExecutorMemory, em) => conf.withExecutorMemory(em)
        case (ExecutorMemoryOverhead, overhead) => conf.withExecutorMemoryOverhead(overhead)
        case (NumExecutors, num) => conf.withNumExecutors(num.toInt)
        case (Queue, q) => conf.withQueue(q)
        case (Conf, c) =>
          val kv = c.split("=", 2)
          if (kv.length == 2) conf.withConf(kv(0), kv(1))
          else throw new IllegalArgumentException(s"$Conf must be specified in key=value format: $c")
      }
      conf
    } else throw new IllegalArgumentException(s"Spark options must be in key-value pairs: ${args.mkString(" ")}")
  }

  private def prependDefaultJavaOptions(defaultJavaOptions: Option[String], javaOptions: Seq[String]): String =
    (defaultJavaOptions ++ javaOptions).mkString(" ").trim
}
