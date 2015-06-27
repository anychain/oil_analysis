# oil  analysis  tools

## converter

Converter is a go tool that used to convert the oil related data from csv and merge these data into output folder

Usage:

* setup the go runtime environment
  
  
* Enter into $GOPATH

```
git clone https://github.com/esse-io/oilanalysis
cd oilanalysis/converter
go build .
./converter  -root=<root_folder> -output=<root_output_folder>
```

* The sum_*.csv are the output files which can be uploaded into hdfs and used by spark analysis.

* Build scala application

Note: you need install the ```sbt``` on your system before build scala.

```
cd oilanalysis/converter/spark
sbt package
```

* Run scala application

```
cd <your spark bin>
. ./load-spark-env.sh && ./spark-submit --executor-memory --packages com.databricks:spark-csv_2.10:1.0.3,com.datastax.spark:spark-cassandra-connector_2.10:1.4.0-M1 --class OilApp <your scala app jar file> <your spark master>
```
