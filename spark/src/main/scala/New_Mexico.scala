import org.apache.spark.sql.SQLContext

object New_Mexico {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Syntax: New_Mexico <Spark Master URL> <HDFS URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("New_Mexico Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val hdfs = args(1)
    val opts = Map("header" -> "true", "delimiter" -> ",")

    val all = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs + "/oil/New_Mexico/AllWells.txt")
    all.registerTempTable("all")

    val county = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs + "/oil/New_Mexico/*.csv")
    county.registerTempTable("county")

    val result = sqlContext.sql("select a.ACRES, a.API, a.OPERATOR, a.PLUG_DATE, a.PRODUCING_POOLID, "
      + "a.PROPERTY, a.RANGE, a.SDIV_UL, a.SECTION, a.SPUD_DATE, a.TOWNSHIP, a.TVD_DEPTH, "
      + "a.water_inj_2013, a.water_inj_2014, a.water_inj_2015, a.water_prod_2013, a.water_prod_2014, "
      + "a.water_prod_2015, a.WELL_NAME, a.WELL_TYPE, a.COMPL_STATUS, a.COUNTY, a.days_prod_2013, "
      + "a.days_prod_2014, a.days_prod_2015, a.ELEVGL, a.EW_CD, a.FTG_EW, a.FTG_NS, a.gas_prod_2013, "
      + "a.gas_prod_2014, a.gas_prod_2015, a.LAND_TYPE, a.LAST_PROD_INJ, a.LATITUDE, a.LONGITUDE, "
      + "a.NBR_COMPLS, a.NS_CD, a.OCD_UL, a.OGRID_CDE, a.oil_prod_2013, a.oil_prod_2014, a.oil_prod_2015, "
      + "a.ONE_PRODUCING_POOL_NAME, "
      + "c.PropertyName, c.WellID, c.OgridName, c.Unit, c.CountyName, "
      + "c.Formation, c.LandType, c.PoolName, c.Oil1, c.Oil2, c.Oil3, c.Oil4, c.Oil5, c.Oil6, c.Oil7, "
      + "c.Oil8, c.Oil9, c.Oil10, c.Oil11, c.Oil12, c.Gas1, c.Gas2, c.Gas3, c.Gas4, c.Gas5, c.Gas6, c.Gas7, "
      + "c.Gas8, c.Gas9, c.Gas10, c.Gas11, c.Gas12, c.Water1, c.Water2, c.Water3, c.Water4, c.Water5, "
      + "c.Water6, c.Water7, c.Water8, c.Water9, c.Water10, c.Water11, c.Water12, c.CO21, c.CO22, c.CO23, "
      + "c.CO24, c.CO25, c.CO26, c.CO27, c.CO28, c.CO29, c.CO210, c.CO211, c.CO212, c.Days1, c.Days2, c.Days3, "
      + "c.Days4, c.Days5, c.Days6, c.Days7, c.Days8, c.Days9, c.Days10, c.Days11, c.Days12, c.Year "
      + "from all a, county c where a.API = c.API")
    result.count()
  }
}
