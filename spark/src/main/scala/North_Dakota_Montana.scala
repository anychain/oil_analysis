import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.hive.HiveContext

object North_Dakota_Montana {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Syntax: North_Dakota_Montana <Spark Master URL> <HDFS URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("North_Dakota_Montana Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val hdfs = args(1)
    sqlContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + "/user/hive/warehouse")

    val opts = Map("header" -> "true", "delimiter" -> "\t")

    // production table
    // North Dakota
    val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("MM-yyyy");
    val schemaString = ("API_No Pool Date OilBBL WaterBBL GasMCF DaysProduced")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val state_prod = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/North_Dakota/production/State*.txt")
    val row_rdd = state_prod.map(p => Row(p(1), p(2),
      new SimpleDateFormat("yyyy/M/1").format(simpleDateFormat.parse(p(3).toString())),
      p(4), p(5), p(6), p(7)))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    df.registerTempTable("state_prod")

    // Montana
    val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");
    val prod_well = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/Montana/histprodwell.tab")
    val row_rdd = prod_well.map(p => Row(p(1), "",
      new SimpleDateFormat("yyyy/M/1").format(simpleDateFormat.parse(p(0).toString())),
      p(7), p(9), p(8), p(10)))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    df.registerTempTable("prod_well")

    val result = sqlContext.sql("select * from state_prod UNION ALL select * from prod_well")
    result.na.drop().write.mode(SaveMode.Overwrite).saveAsTable("North_Dakota_Montana_production")

    // wellindex table
    // North Dakota
    val opts = Map("header" -> "true", "delimiter" -> "\t")
    val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");
    val schemaString = ("API_No CurrentOperator CurrentWellName LeaseName LeaseNumber "
      + "ApprovedDate SpudDate CompletionDate TD CountyName FieldName ProducedPools "
      + "OilWaterGasCums IPTDateOilWaterGas Slant Latitude Longitude WellType WellStatus "
      + "WellStatusDate")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val well_index = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/North_Dakota/WellIndex.txt")
    val temp = well_index.na.replace(Array("SpudDate", "WellStatusDate"),
      Map("Confidential" -> "1/1/1800",
      "" -> "1/1/1800")).na.fill(Map("SpudDate" -> "1/1/1800"))
    val row_rdd = temp.map(p => Row(p(0), p(2), p(3), p(4), p(5), "",
      new SimpleDateFormat("M/d/yyyy").format(simpleDateFormat.parse(p(8).toString())),
      "", p(9), p(10), p(16), p(17), p(18), p(19), p(20), p(21), p(22), p(23), p(24),
     new SimpleDateFormat("M/d/yyyy").format(simpleDateFormat.parse(p(26).toString()))))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    df.na.replace("Slant", Map("VERTICAL" -> "Vertical",
      "HORIZONTAL RE-ENTRY" -> "Horizontal Re-drill/Re-entry",
      "DIRECTIONAL" -> "Directional",
      "HORIZONTAL" -> "Horizontal")).registerTempTable("well_index")

    // Montana
    val opts = Map("header" -> "true", "delimiter" -> "\t")
    val well_data = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/Montana/histWellData.tab")
    val result = well_data.na.replace("Slant", Map("V" -> "VERTICAL", "D" -> "DIRECTIONAL", "H" -> "HORIZONTAL",
      "R" -> "HORIZONTAL RE-DRILL/RE-ENTRY", "X" -> "VERTICAL (CANCELED HORIZ.)", 
      "Y" -> "DIRECTIONAL (CANCELED HORIZ.)", "Z" -> "VERTICAL (CANCELED HORIZ. RE-DRILL)"))

    val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    val temp = result.na.replace(Array("Dt_APD", "Dt_Cmp"), Map("" -> "1800-1-1"))
    val row_rdd = temp.map(p => Row(p(0), p(2), p(3), p(22), p(18), p(26), p(4), p(6)
      , new SimpleDateFormat("M/d/yyyy").format(simpleDateFormat.parse(p(23).toString()))
      , new SimpleDateFormat("M/d/yyyy").format(simpleDateFormat.parse(p(24).toString()))
      ))
    val schemaString = ("API_No CoName Well_Nm Stat_Field Slant DTD Well_Typ Wl_Status Dt_APD Dt_Cmp")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    df.registerTempTable("well_data_temp")

    val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");
    val prod_well = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/Montana/histprodwell.tab")
    val row_rdd = prod_well.map(p => Row(
      new SimpleDateFormat("MM/yyyy").format(simpleDateFormat.parse(p(0).toString())),
      p(1)))
    val schemaString = ("Month API_No")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    df.groupBy($"API_No").agg(max($"Month") as "Month").registerTempTable("prod_well_tmp")
    val row_rdd = prod_well.map(p => Row(
      new SimpleDateFormat("MM/yyyy").format(simpleDateFormat.parse(p(0).toString())),
      p(1), p(4)))
    val schemaString = ("Month API_No Lease_Unit")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    df.registerTempTable("prod_well")

    val opts = Map("header" -> "true", "delimiter" -> "\t")
    val well_surface = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/Montana/Well_Surface_Longitude_latitude.tab")
    val row_rdd = well_surface.map(p => Row(p(0).toString().replace("-", ""), p(1), p(2)))
    val schemaString = ("API Wh_Long Wh_Lat")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    df.registerTempTable("well_surface")

    val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");
    val opts = Map("header" -> "true", "delimiter" -> ",")
    val search_well = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs
      + "/oil/Montana/Search_Wells.csv")
    val schemaString = ("API_No SpudDate CountyName")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val row_rdd = search_well.map(p => Row(p(0).toString().replace("-", ""),
      p(7),
      p(5)))
    val df = sqlContext.createDataFrame(row_rdd, schema)
    val temp = df.na.replace("SpudDate", Map("" -> "1/1/1800")).na.fill(Map("SpudDate" -> "1/1/1800"))
    val row_rdd = temp.map(p => Row(p(0),
      new SimpleDateFormat("M/d/yyyy").format(simpleDateFormat.parse(p(1).toString())),
      p(2)))
    sqlContext.createDataFrame(row_rdd, temp.schema).registerTempTable("search_well")

    val result = sqlContext.sql("select wt.API_No as API_No, wt.CoName as CurrentOperator, "
      + "wt.Well_Nm as CurrentWellName, '' as LeaseName, p.Lease_Unit as LeaseNumber, "
      + "wt.Dt_APD as ApprovedDate, s.SpudDate as SpudDate, wt.Dt_Cmp as CompletionDate, wt.DTD as TD, "
      + "s.CountyName as CountyName, wt.Stat_Field as FieldName, '' as ProducedPools, "
      + "'' as OilWaterGasCums, '' as IPTDateOilWaterGas, wt.Slant as Slant, "
      + "f.Wh_Long as Latitude, f.Wh_Lat as Longitude, wt.Well_Typ as WellType, wt.Wl_Status as WellStatus, "
      + "'' as WellStatusDate "
      + "from well_data_temp wt, prod_well_tmp pt, prod_well p, well_surface f, search_well s "
      + "where wt.API_No == pt.API_No and p.API_No = pt.API_No and pt.API_No = f.API "
      + "and s.API_No = pt.API_No and pt.Month = p.Month")
    result.registerTempTable("hist_well")

    val tables = sqlContext.sql("select * from well_index UNION ALL select * from hist_well")
    val newTables = tables.na.replace("SpudDate", Map("1/1/1800" -> ""))
    newTables.na.drop().write.mode(SaveMode.Overwrite).saveAsTable("North_Dakota_Montana_index")
  }
}