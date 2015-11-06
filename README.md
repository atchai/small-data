# Small Data

Example code and data for small data blog post on [atchai.com](http://atchai.com). See [the blog post](http://atchai.com/blog/your-big-data-might-be-small/) for more details.

## Requirements

* [MongoDB v3.0.9](https://www.mongodb.com/)
* [ElasticSearch v1.7.3](https://www.elastic.co/products/elasticsearch)
* [Fabric v1.8](http://www.fabfile.org/)

For the Spark implementation:

* [Spark v1.5](http://spark.apache.org/)
* [sbt v0.3.9](http://www.scala-sbt.org/)

For the C++ implementation:

* [CMake v3.3](https://cmake.org/)
* [libbson v1.1.7](https://github.com/mongodb/libbson)

To install all dependencies on OS X using [Homebrew](http://brew.sh/) run:
```
brew install mongodb elasticsearch fabric spark sbt cmake libbson
```

## Running

To run the Spark process (locally) use:
```
fab spark_run
```

To run the C++ process use:
```
fab cpp_run
```
