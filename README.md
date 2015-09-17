Kafka Graphite Metrics Reporter
===============================

Install
-------
Maven
``` xml
<dependency>
  <groupId>com.pakulov.kafka</groupId>
  <artifactId>kafka_2.10-graphite</artifactId>
  <version>0.1.3</version>
</dependency>
```

Gradle
``` groovy
compile 'com.pakulov.kafka:kafka_2.10-graphite:0.1.3'
```

Build
-----
Current plugin could be compiled with different Scala versions, use *scalaVersion* Gradle's property to define version

```
./gradlew -PscalaVersion=2.10.5 build 
```

There is also a way to build a deb package

```
./gradlew buildDeb
```

Usage
-----
At first you have to configure kafka reporters *server.properties* file

* `kafka.metrics.reporters=kafka.metrics.KafkaGraphiteMetricsReporter`
* `kafka.metrics.polling.interval.secs=60`: Polling interval that will be used for all Kafka metrics

Plugin has a set of possible parameters:
* `kafka.graphite.metrics.reporter.enabled`: Enables actual plugin (default: false)
* `kafka.graphite.metrics.host`: The graphite host to connect to (default: localhost)
* `kafka.graphite.metrics.port`: The port to connect to (default: 2003)
* `kafka.graphite.metrics.prefix`: The metric prefix that's sent with metric names (default: kafka)
* `kafka.graphite.metrics.include`: A regular expression allowing explicitly include certain metrics (default: null)
* `kafka.graphite.metrics.exclude`: A regular expression allowing you to exclude certain metrics (default: null)
* `kafka.graphite.metrics.jvm.enabled`: Controls JVM metrics output (default: true)