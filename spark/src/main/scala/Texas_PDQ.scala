import org.apache.spark.sql.SQLContext

object Texas_PDQ {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Syntax: Texas_PDQ <Spark Master URL> <HDFS URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Texas_PDQ Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val opts = Map("header" -> "true", "delimiter" -> ",")

    val lease_cycle = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Texas/PDQ_OWNR.og_lease_cycle.csv")
    lease_cycle.registerTempTable("lease_cycle")

    val well_completion = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Texas/PDQ_OWNR.og_well_completion.csv")
    well_completion.registerTempTable("well_completion")

    val result = sqlContext.sql("select l.OIL_GAS_CODE, l.DISTRICT_NO, l.LEASE_NO, l.CYCLE_YEAR, l.CYCLE_MONTH, l.CYCLE_YEAR_MONTH, l.LEASE_NO_DISTRICT_NO, l.OPERATOR_NO, l.FIELD_NO, l.FIELD_TYPE, l.GAS_WELL_NO, l.PROD_REPORT_FILED_FLAG, l.LEASE_OIL_PROD_VOL, l.LEASE_OIL_ALLOW, l.LEASE_OIL_ENDING_BAL, l.LEASE_GAS_PROD_VOL, l.LEASE_GAS_ALLOW, l.LEASE_GAS_LIFT_INJ_VOL, l.LEASE_COND_PROD_VOL, l.LEASE_COND_LIMIT, l.LEASE_COND_ENDING_BAL, l.LEASE_CSGD_PROD_VOL, l.LEASE_CSGD_LIMIT, l.LEASE_CSGD_GAS_LIFT, l.LEASE_OIL_TOT_DISP, l.LEASE_GAS_TOT_DISP, l.LEASE_COND_TOT_DISP, l.LEASE_CSGD_TOT_DISP, l.DISTRICT_NAME, l.LEASE_NAME, l.OPERATOR_NAME, l.FIELD_NAME, w.WELL_NO, w.API_COUNTY_CODE, w.API_UNIQUE_NO, w.ONSHORE_ASSC_CNTY, w.COUNTY_NAME, w.OIL_WELL_UNIT_NO, w.WELL_ROOT_NO, w.WELLBORE_SHUTIN_DT, w.WELL_SHUTIN_DT, w.WELL_14B2_STATUS_CODE, w.WELL_SUBJECT_14B2_FLAG, w.WELLBORE_LOCATION_CODE from lease_cycle l, well_completion w where l.LEASE_NO = w.LEASE_NO and l.DISTRICT_NAME = w.DISTRICT_NAME")
    result.count()
  }
}