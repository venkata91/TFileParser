# TFileParser
Parse TFiles created through Yarn log aggregation. Yarn Log aggregation uses a special file format to aggregate the logs which is called TFile. Many a times if you need to parse the application logs, there is no easy way to do it. 

This library with Spark can make it easier.

## To compile:
```
mvn clean install
```

## To run with Spark: 
```
bin/spark-shell --jars TFileParser-1.0-SNAPSHOT.jar
```

```
import com.qubole.rdd._

val path = "s3://application_1513024130439_0003/"

// Create a TFile RDD from the Yarn aggregated log path
val tfile = TFileRDD(sc, path)

// Convert the RDD[(Text, Text)] to RDD[(String, String)] as Hadoop Text type is not serializable therefore you cannot do collect, take etc. Mapping it to RDD[(String, String)] makes it easier
val tfilestr = tfile.map(x => (x._1.toString, x._2.toString))

// Create an RDD to filter lines based on ERROR logs
val error = tfilestr.filter(x => x._2.contains("ERROR"))

// Count of error log lines
error.count
```

## Contributions
Please file issues if you need any other functionalities to be supported. We also encourage contributions back.
