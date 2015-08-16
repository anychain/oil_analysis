import org.apache.spark.sql.SQLContext

object Pennsylvania {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Syntax: Pennsylvania <Spark Master URL> <HDFS URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Pennsylvania Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val opts = Map("header" -> "true", "delimiter" -> ",")

    val pen_wells = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Pennsylvania/*.csv")
    pen_wells.registerTempTable("pen_wells")

    pen_wells.count()
  }
}
