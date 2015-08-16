import org.apache.spark.sql.SQLContext

object Alaska_Well {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Syntax: Alaska_Well <Spark Master URL> <HDFS URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Alaska_Well Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val opts = Map("header" -> "true", "delimiter" -> ",")

    val master = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Alaska/tblWellMaster.txt")
    master.registerTempTable("master")

    //val injection = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Alaska/tblWellPoolInjection.txt")
    //injection.registerTempTable("injection")

    val production = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Alaska/tblWellPoolProduction.txt")
    production.registerTempTable("production")

    val result = sqlContext.sql("select p.ReportDate, m.API_WellNo, m.PermitNumber, m.OpNo, m.WellName, m.WellNumber, m.WellUnit, "
        + " m.CurrentClass, m.CurrentStatus, m.CurrentDateStatus, m.PermitClass, m.PermitStatus, m.CompClass, "
        + " m.CompStatus, m.DTD, m.TVD, m.Sales_Cd, m.Actg_Grp, m.Mult_Latrl, m.CompNum, m.Field_No,"
        + " m.Wh_GeoArea, m.Wh_Lat, m.Wh_Long, m.Wh_FtNS, m.Wh_Ns, m.Wh_FtEW, m.Wh_EW, m.Wh_Sec, m.Wh_Twpn,"
        + " m.Wh_Twpd, m.Wh_RngN, m.Wh_RngD, m.Wh_Pm, m.Wh_OnOffShore, m.Elev_Gr, m.Elev_KB, m.Elev_DF, m.Elev_RT,"
        + " m.Elev_CF, m.Elev_OF, m.Elev_CB, m.WtrDepth, m.BH_FtNS, m.BH_NS, m.BH_FtEW, m.BH_EW, m.BH_Sec,"
        + " m.BH_Twpn, m.BH_Twpd, m.BH_RngN, m.BH_RngD, m.BH_PM, m.Dt_Mod, m.Confidential, m.Release_Date,"
        + " p.FieldPoolCode, p.WellType, p.MethodOper, p.ProdOil, p.ProdGas, p.ProdWater, p.ProdDays, p.TbgPsi,"
        + " p.CsgPsi, p.DateMod "
        + " from master m, production p where p.API_WellNo = m.API_WellNo")
    result.count()
  }
}
