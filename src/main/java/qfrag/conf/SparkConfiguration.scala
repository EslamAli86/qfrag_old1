package qfrag.conf

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.spark.SparkConf

import scala.collection.mutable.Map

import qfrag.utils.SerializableConfiguration

/**
 * Configurations are passed along in this mapping
 */
case class SparkConfiguration (confs: Map[String,Any])
    extends qfrag.conf.Configuration {

  def this() {
    this (Map.empty)
  }

  /**
   * Sets a configuration (mutable)
   */
  def set(key: String, value: Any): SparkConfiguration = {
    confs.put (key, value)
    fixAssignments
    this
  }

  /**
   * Sets a configuration (mutable) if this configuration has not been set yet
   */
  def setIfUnset(key: String, value: Any): SparkConfiguration = confs.get(key) match {
    case Some(_) =>
      this
    case None =>
      set (key, value)
  }

  /**
   * Sets a configuration (immutable)
   */
  def withNewConfig(key: String, value: Any): SparkConfiguration = {
    val newConfig = this.copy (confs = confs ++ Map(key -> value))
    newConfig.fixAssignments
    newConfig
  }

  /**
   * Sets a default hadoop configuration for this arabesque configuration. That
   * way both can be shipped together to the workers.
   * This function mutates the object.
   */
  def setHadoopConfig(conf: HadoopConfiguration): SparkConfiguration = {
    val serHadoopConf = new SerializableConfiguration(conf)
    // we store the hadoop configuration as a common configuration
    this.confs.put (SparkConfiguration.HADOOP_CONF, serHadoopConf)
    this
  }

  /**
   * Returns a hadoop configuration assigned to this configuration, or throw an
   * exception otherwise.
   */
  def hadoopConf: HadoopConfiguration = confs.get(SparkConfiguration.HADOOP_CONF) match {
    case Some(serHadoopConf: SerializableConfiguration) =>
      serHadoopConf.value

    case Some(value) =>
      logError (s"The hadoop configuration type is invalid: ${value}")
      throw new RuntimeException(s"Invalid hadoop configuration type")

    case None =>
      logError ("The hadoop configuration type is not set")
      throw new RuntimeException(s"Hadoop configuration is not set")
  }

  /**
   * Translates Arabesque configuration into SparkConf.
   * ATENTION: This is highly spark-dependent
   */
  def sparkConf = {
    if (!isInitialized) {
      initialize()
    }
    val sparkMaster = getString ("spark_master", "local[*]")
    val conf = new SparkConf().
      setAppName ("Arabesque Master Execution Engine").
      setMaster (sparkMaster)
        
    conf.set ("spark.executor.memory", getString("worker_memory", "1g"))
    conf.set ("spark.driver.memory", getString("driver_memory", "1g"))

    sparkMaster match {
      case "yarn-client" | "yarn-cluster" | "yarn" =>
        conf.set ("spark.executor.instances", getInteger("num_workers", 1).toString)
        conf.set ("spark.executor.cores", getInteger("num_compute_threads", 1).toString)
        conf.set ("spark.driver.cores", getInteger("num_compute_threads", 1).toString)

      case standaloneUrl : String if standaloneUrl startsWith "spark://" =>
        conf.set ("spark.cores.max",
          (getInteger("num_workers", 1) * getInteger("num_compute_threads", 1)).toString)

      case _ =>
    }
    logInfo (s"Spark configurations:\n${conf.getAll.mkString("\n")}")
    conf
  }

  /**
   * We assume the number of requested executor cores as an alternative number of
   * partitions. However, by the time we call this function, the config *num_partitions*
   * should be already set by the user, or by the execution master engine which
   * has SparkContext.defaultParallelism as default
   */
  def numPartitions: Int = {
    /*
    val numOfWorkers = getInteger("num_workers", 1)
    val numOfThreads = getInteger("num_compute_threads", Runtime.getRuntime.availableProcessors)
    val numOfPartitions = getInteger("num_partitions", numOfWorkers * numOfThreads)

    println(s"numOfWorkers=$numOfWorkers")
    println(s"numOfThreads=$numOfThreads")
    println(s"numOfPartitions=$numOfPartitions")

    numOfPartitions
    */
    //*
    getInteger("num_partitions",
      getInteger("num_workers", 1) *
        getInteger("num_compute_threads", Runtime.getRuntime.availableProcessors))
    //*/
  }
  /**
   * Given the total number of partitions in the cluster, this function returns
   * roughly the number of partitions per worker. We assume an uniform division
   * among workers.
   */
  def numPartitionsPerWorker: Int = numPartitions / getInteger("num_workers", 1)

  /**
   * Update assign internal names to user defined properties
   */
  private def fixAssignments = {
    def updateIfExists(key: String, config: String) = confs.remove (key) match {
      case Some(value) => confs.put (config, value)
      case None =>
    }
    // log level
    updateIfExists ("log_level", Configuration.CONF_LOG_LEVEL)

    // communication strategy
    updateIfExists ("comm_strategy", Configuration.CONF_COMM_STRATEGY)

    // odag flush method
    updateIfExists ("flush_method", Configuration.CONF_ODAG_FLUSH_METHOD)
    updateIfExists ("num_odag_parts", Configuration.CONF_EZIP_AGGREGATORS)

    // input
    updateIfExists ("input_graph_path", Configuration.CONF_MAINGRAPH_PATH)
    updateIfExists ("input_graph_local", Configuration.CONF_MAINGRAPH_LOCAL)
 
    // output
    updateIfExists ("output_active", Configuration.CONF_OUTPUT_ACTIVE)
    updateIfExists ("output_path", Configuration.CONF_OUTPUT_PATH)

    // aggregation
    updateIfExists ("incremental_aggregation", Configuration.CONF_INCREMENTAL_AGGREGATION)
   
    // max number of odags in case of odag communication strategy
    updateIfExists ("max_odags", Configuration.CONF_COMM_STRATEGY_ODAGMP_MAX)

  }

  /**
   * Garantees that arabesque configuration is properly set
   *
   * TODO: generalize the initialization in the superclass Configuration
   */
  override def initialize(): Unit = synchronized {
    if (Configuration.isUnset || uuid != Configuration.get[SparkConfiguration].uuid) {
      initializeInJvm()
      Configuration.set (this)
    }
  }

  /**
   * Called whether no arabesque configuration is set in the running jvm
   */
  private def initializeInJvm(): Unit = {

    fixAssignments

    // common configs

    setAggregationsMetadata (new java.util.HashMap())

    setOutputPath (getString(Configuration.CONF_OUTPUT_PATH, Configuration.CONF_OUTPUT_PATH_DEFAULT))


    initialized = true
  }

  def getValue(key: String, defaultValue: Any): Any = confs.get(key) match {
    case Some(value) =>
//      println(s"default value for $key is $defaultValue and the value found in configuration is $value")
      value
    case None =>
//      println(s"Value for $key not found, replaced with $defaultValue")
      defaultValue
  }

  override def getInteger(key: String, defaultValue: Integer) =
    getValue(key, defaultValue).asInstanceOf[Int]

  override def getString(key: String, defaultValue: String) =
    getValue(key, defaultValue).asInstanceOf[String]
  
  override def getBoolean(key: String, defaultValue: java.lang.Boolean) =
    getValue(key, defaultValue).asInstanceOf[Boolean]

}

object SparkConfiguration {
  def get: SparkConfiguration = {
    Configuration.get[SparkConfiguration]
  }

  // odag flush methods
  val FLUSH_BY_PATTERN = "flush_by_pattern" // good for regular distributions
  val FLUSH_BY_ENTRIES = "flush_by_entries" // good for irregular distributions but small embedding domains
  val FLUSH_BY_PARTS = "flush_by_parts"     // good for irregular distributions, period

  // communication strategies
  val COMM_ODAG_SP = "odag_sp"              // pack embeddings with single-pattern odags
  val COMM_ODAG_MP = "odag_mp"              // pack embeddings with multi-pattern odags
  val COMM_EMBEDDING = "embedding"          // pack embeddings with compressed caches (e.g., LZ4)
  val COMM_SIMPLE_STORAGE = "simple_storage"
  val COMM_SIMPLE_STORAGE_SP = "simple_storage_sp"

  // hadoop conf
  val HADOOP_CONF = "hadoop_conf"

  // computation container
  val COMPUTATION_CONTAINER = "computation_container"
  val MASTER_COMPUTATION_CONTAINER = "master_computation_container"
}
