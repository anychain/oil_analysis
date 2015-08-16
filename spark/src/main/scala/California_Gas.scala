import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object California_Gas {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Syntax: California_Gas <Spark Master URL> <HDFS URL>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("California_Gas Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val opts = Map("header" -> "false", "delimiter" -> ",")

    val schemaString = ("DistrictNumber FieldCode AreaCode APINumber WellStatus Section Subsection Township "
        + "Range BaseMeridian OperatorCode LeaseName WellNumber FieldName AreaName OperatorName OperatorStatus "
        + "OperatorReportingMethod CountyName PoolCode WellTypeCode PoolWellTypeStatus SystemEntryDate PoolName "
        + "PWT__ID")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val gas_wells = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/California/*GasWells.csv")
    val df = sqlContext.createDataFrame(gas_wells.rdd, schema)
    df.registerTempTable("gas_wells")

    val schemaString = ("ProductionDate ProductionStatus CasingPressure TubingPressure BTUofGasProduced MethodOfOperation "
        + "APIGravityOfOil WaterDisposition OilorCondensateProduced DaysProducing GasProduced_MCF WaterProduced_BBL "
        + "MissingDataCode PWT__ID")
    val schema = StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val production = sqlContext.read.format("com.databricks.spark.csv").options(opts).load(args(1) + "/oil/California/*GasWellProduction.csv")
    val df = sqlContext.createDataFrame(production.rdd, schema)
    df.registerTempTable("production")

    val result = sqlContext.sql("select g.DistrictNumber, g.FieldCode, g.AreaCode, g.APINumber, g.WellStatus, "
        + "g.Section, g.Subsection, g.Township, g.Range, g.BaseMeridian, g.OperatorCode, g.LeaseName, "
        + "g.WellNumber, g.FieldName, g.AreaName, g.OperatorName, g.OperatorStatus, g.OperatorReportingMethod, "
        + "g.CountyName, g.PoolCode, g.WellTypeCode, g.PoolWellTypeStatus, g.SystemEntryDate, g.PoolName, "
        + "g.PWT__ID, p.ProductionDate, p.ProductionStatus, p.CasingPressure, p.TubingPressure, "
        + "p.BTUofGasProduced, p.MethodOfOperation, p.APIGravityOfOil, p.WaterDisposition, "
        + "p.OilorCondensateProduced, p.DaysProducing, p.GasProduced_MCF, p.WaterProduced_BBL, p.MissingDataCode "
        +" from gas_wells g, production p where p.PWT__ID = g.PWT__ID")
    result.count()
  }
}
