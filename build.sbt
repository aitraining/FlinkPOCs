name := "sparkProjects"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.9.1"
libraryDependencies ++= Seq(
 // https://mvnrepository.com/artifact/org.apache.flink/flink-scala
 "org.apache.flink" %% "flink-scala" % flinkVersion,
"org.apache.flink" %% "flink-connector-filesystem" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-core
 "org.apache.flink" % "flink-core" % flinkVersion,

 // https://mvnrepository.com/artifact/org.apache.flink/flink-table-planner
 "org.apache.flink" %% "flink-table-planner" % flinkVersion,

 // https://mvnrepository.com/artifact/org.apache.flink/flink-parent
 "org.apache.flink" % "flink-parent" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-table
 "org.apache.flink" % "flink-table" % flinkVersion,

 //if unable to download depenency download from this link and add manually.
 //https://repo1.maven.org/maven2/org/apache/flink/flink-table-api-scala-bridge_2.11/1.8.0/flink-table-api-scala-bridge_2.11-1.8.0.jar

 //"org.apache.flink" %% "flink-table-api-scala-bridge_2.11" % flinkVersion,

 // https://mvnrepository.com/artifact/org.apache.flink/flink-table-common
 "org.apache.flink" % "flink-table-common" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-table-api-scala
 "org.apache.flink" %% "flink-table-api-scala" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-clients
 "org.apache.flink" %% "flink-clients" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-csv
"org.apache.flink" % "flink-csv" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-jdbc
 "org.apache.flink" %% "flink-jdbc" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-table-api-scala-bridge
 "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-table-runtime-blink
 "org.apache.flink" %% "flink-test-utils" % flinkVersion,
// https://mvnrepository.com/artifact/org.apache.flink/flink-table-uber-blink
"org.apache.flink" %% "flink-table-uber-blink" % flinkVersion,
// https://mvnrepository.com/artifact/org.apache.flink/flink-table-uber
"org.apache.flink" %% "flink-table-uber" % flinkVersion,
  "org.apache.flink" %% "flink-table-runtime-blink" % flinkVersion,
 "org.apache.flink" % "flink-json" % flinkVersion,
 // https://mvnrepository.com/artifact/org.apache.flink/flink-json
 "org.apache.flink" % "flink-json" % flinkVersion,
   // https://mvnrepository.com/artifact/org.apache.flink/flink-table-planner-blink
 "org.apache.flink" %% "flink-table-planner-blink" % flinkVersion
)
// https://mvnrepository.com/artifact/org.apache.flink/flink-table
libraryDependencies += "org.apache.flink" % "flink-table" % "1.9.1" % "provided" pomOnly()

// https://mvnrepository.com/artifact/org.apache.flink/flink-statebackend-rocksdb
libraryDependencies += "org.apache.flink" %% "flink-statebackend-rocksdb" % "1.9.1" % Test
// https://mvnrepository.com/artifact/org.apache.flink/flink-sql-parser
libraryDependencies += "org.apache.flink" % "flink-sql-parser" % "1.9.1"

// https://mvnrepository.com/artifact/org.apache.calcite/calcite-core
libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.21.0"


// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
//
// libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.8.0"
