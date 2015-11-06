name := "SmallData"
version := "1.0"
scalaVersion := "2.10.4"
retrieveManaged := true

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"

// Scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

// MongoDB
libraryDependencies += "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.3.2"

// ElasticSearch
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0.Beta4"
