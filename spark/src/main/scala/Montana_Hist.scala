import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

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
    val opts = Map("header" -> "true", "delimiter" -> "\t")

    val lease_prod = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Montana/histLeaseProd.tab")
    lease_prod.registerTempTable("lease_prod")

    val prod_well = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Montana/histprodwell.tab")
    prod_well.registerTempTable("prod_well")

    val well_data = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Montana/histWellData.tab")
    well_data.registerTempTable("well_data")

    opts = Map("header" -> "true", "delimiter" -> ",")

    val df = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/Montana/Search_Wells.csv")
    val schema = df.schema
    val row_rdd = df.map(p => Row(p(0).toString.replace("-", ""), p(1), p(2), p(3), p(4), p(5), p(6), p(7),
      p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22),
      p(23), p(24), p(25), p(26), p(27), p(28)))

    val search_well = sqlContext.createDataFrame(rdd, schema)
    search_well.registerTempTable("search_well")

    val result = sqlContext.sql("select p.rpt_date, p.API_WELLNO, p.ST_FMTN_CD, p.Name_, p.Lease_Unit, p.OPNO, "
      + "p.CoName, p.BBLS_OIL_COND, p.MCF_GAS, p.BBLS_WTR, p.DAYS_PROD, p.AMND_RPT, p.STATUS, p.dt_mod, "
      + "l.Dt_Receive, l.Del_Rpt, l.Amnd_Rpt, l.OpNo, l.StartIvn_OilCd, l.Oil_Prod, l.Gas_Prod, l.Wtr_Prod, "
      + "l.Oil_Sold, l.Gas_Sold, l.OilSpill, l.WtrSpill, l.FlarVnt_Gas, l.UseOil, l.UseGas, l.OilInj, l.GasInj, "
      + "l.WtrInj, l.WtrTo_Pit, l.Other_Oil, l.Other_Gas, l.Other_Wtr, l.Dt_Amend, l.Lease_Update, l.No_ProdWells, "
      + "l.No_SIWells, l.Dt_Mod, "
      + "w.Well_Nm, w.Well_Typ, w.Type, w.Wl_Status, w.Status, w.Wh_Sec, w.Wh_Twpn, w.Wh_Twpd, w.Wh_RngN, w.Wh_RngD, "
      + "w.Wh_Qtr, w.Wh_FtNS, w.Wh_Ns, w.Wh_FtEW, w.Wh_EW, w.Slant, w.Reg_Field_No, w.Reg_Field, w.Stat_Field_No, "
      + "w.Stat_Field, w.Dt_APD, w.Dt_Cmp, w.Elev_KB, w.DTD, "
      + "s.API_NO, s.County, s.Field, s.Dt_Spud, s.Wh_TR, s.WH_TwpN, s.WH_TwpD, s.Elev_Gr, s.Elev_DF, s.PB_MD, "
      + "s.NextInspection, s.Cnty, s.OrigTyp "
      + "from prod_well p, lease_prod l, well_data w, search_well s where p.API_WELLNO = w.API_WellNo "
      + " and p.API_WELLNO = s.API_NO and p.Lease_Unit = l.Lease_Unit and p.rpt_date = l.Rpt_Date")
    result.count()
  }
}