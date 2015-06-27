import org.apache.spark.sql.SQLContext

object OilApp {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Syntax: OilApp <Spark Master URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Oil Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val opts = Map("header" -> "true")

    val production = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("/oil/sum_production.csv")
    val lease = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("/oil/sum_lease.csv")
    val disposition_gas = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("/oil/sum_disposition_gas.csv")
    val disposition_oil = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("/oil/sum_disposition_oil.csv")
    production.registerTempTable("production")
    lease.registerTempTable("lease")
    disposition_gas.registerTempTable("disposition_gas")
    disposition_oil.registerTempTable("disposition_oil")
    val result = sqlContext.sql("select production.District, production.Date, production.LeaseNo, production.WellNo, production.WellType, lease.OperNo, lease.OperName, lease.LeaseName from production, lease where production.District = lease.District and production.LeaseNo = lease.LeaseNo and production.Date = lease.Date")
    result.count()
  }
}
