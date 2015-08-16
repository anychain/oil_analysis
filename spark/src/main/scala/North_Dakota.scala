import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object Montana_Hist {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Syntax: Montana_Hist <Spark Master URL> <HDFS URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Montana_Hist Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val hdfs = args(1)

    sqlContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + "/user/hive/warehouse")
    val opts = Map("header" -> "true", "delimiter" -> "\t")
    val log_tops = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs 
      + "/oil/North_Dakota/LogTops.txt")
    log_tops.write.mode(SaveMode.Overwrite).saveAsTable("North_Dakota_log_tops")

    val well_index = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/North_Dakota/WellIndex.txt")
    well_index.write.mode(SaveMode.Overwrite).saveAsTable("North_Dakota_well_index")

    val production = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/North_Dakota/production/State*.txt")
    val schemaString = ("File_No API_No Pool Date BBLS_Oil BBLS_Water MCF_Gas "
      + "Days_Produced Oil_Sold MCF_Sold MCF_Flared")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val df = sqlContext.createDataFrame(production.rdd, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable("North_Dakota_production")
  }
}