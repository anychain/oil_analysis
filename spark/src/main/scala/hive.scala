
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.SaveMode

sqlContext.sql("SET hive.metastore.warehouse.dir=hdfs://arahant-01:9000/user/hive/warehouse")

val opts = Map("header" -> "true", "delimiter" -> ",")

val texas_lease_cycle = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Texas/PDQ_OWNR.og_lease_cycle.csv")
texas_lease_cycle.write.mode(SaveMode.Overwrite).saveAsTable("texas_lease_cycle")

val texas_well_completion = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Texas/PDQ_OWNR.og_well_completion.csv")
texas_well_completion.write.mode(SaveMode.Overwrite).saveAsTable("texas_well_completion")


    val opts = Map("header" -> "true", "delimiter" -> "\t")

    val lease_prod = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Montana/histLeaseProd.tab")
    lease_prod.write.mode(SaveMode.Overwrite).saveAsTable("montana_lease_prod")

    val prod_well = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Montana/histprodwell.tab")
    prod_well.write.mode(SaveMode.Overwrite).saveAsTable("montana_prod_well")

    val well_data = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Montana/histWellData.tab")
    well_data.write.mode(SaveMode.Overwrite).saveAsTable("montana_well_data")

    val opts = Map("header" -> "true", "delimiter" -> ",")

    val df = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Montana/Search_Wells.csv")
    val schema = df.schema
    val row_rdd = df.map(p => Row(p(0).toString.replace("-", ""), p(1), p(2), p(3), p(4), p(5), p(6), p(7),
      p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22),
      p(23), p(24), p(25), p(26), p(27), p(28)))

    val search_well = sqlContext.createDataFrame(row_rdd, schema)
    search_well.write.mode(SaveMode.Overwrite).saveAsTable("montana_search_well")


    val opts = Map("header" -> "false", "delimiter" -> ",")

    val schemaString = ("DistrictNumber FieldCode AreaCode APINumber WellStatus Section Subsection Township "
        + "Range BaseMeridian OperatorCode LeaseName WellNumber FieldName AreaName OperatorName OperatorStatus "
        + "OperatorReportingMethod CountyName PoolCode WellTypeCode PoolWellTypeStatus SystemEntryDate PoolName "
        + "PWT__ID")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val gas_wells = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/California/*GasWells.csv")
    val df = sqlContext.createDataFrame(gas_wells.rdd, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable("california_gas_wells")

    val schemaString = ("ProductionDate ProductionStatus CasingPressure TubingPressure BTUofGasProduced MethodOfOperation "
        + "APIGravityOfOil WaterDisposition OilorCondensateProduced DaysProducing GasProduced_MCF WaterProduced_BBL "
        + "MissingDataCode PWT__ID")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val production = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/California/*GasWellProduction.csv")
    val df = sqlContext.createDataFrame(production.rdd, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable("california_production")

    val opts = Map("header" -> "true", "delimiter" -> ",")

    val master = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Alaska/tblWellMaster.txt")
    master.write.mode(SaveMode.Overwrite).saveAsTable("alaska_master")

    val production = sqlContext.read.format("com.databricks.spark.csv").options(opts).load("hdfs://arahant-01:9000/oil/Alaska/tblWellPoolProduction.txt")
    production.write.mode(SaveMode.Overwrite).saveAsTable("alaska_production")

    val hdfs = args(1)
    val opts = Map("header" -> "true", "delimiter" -> ",")

    val all = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs + "/oil/New_Mexico/AllWells.txt")
    all.write.mode(SaveMode.Overwrite).saveAsTable("New_Mexico_all")

    val county = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs + "/oil/New_Mexico/*.csv")
    county.write.mode(SaveMode.Overwrite).saveAsTable("New_Mexico_county")
