/**
 * Copyright 2015 Alexander Pakulov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.metrics

import java.net._
import java.util.Properties

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.MetricName
import kafka.utils.VerifiableProperties
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

class GraphiteMockServer extends Thread {
  var socket: Socket = null
  var content: ListBuffer[String] = new ListBuffer[String]
  val server: ServerSocket = new ServerSocket(Random.nextInt(65000))
  val port: Int = server.getLocalPort

  override def run(): Unit = {
    while (!server.isClosed) {
      try {
        socket = server.accept()
        val iterator = Source.fromInputStream(socket.getInputStream).getLines()
        while (iterator.hasNext) {
          content += iterator.next
        }
      } catch {
        case e: SocketException => "Socket closed, good bye.."
      }
    }
  }

  def close(): Unit = {
    server.close()
  }
}

class KafkaGraphiteMetricsReporterTest extends FlatSpec with Matchers with BeforeAndAfter {
  var graphiteServer: GraphiteMockServer = null

  before {
    graphiteServer = new GraphiteMockServer
    graphiteServer.start()
  }

  after {
    graphiteServer.close()
  }

  it should "reportCounters" in {
    val props = new Properties
    props.put("kafka.metrics.reporters", "kafka.metrics.KafkaGraphiteMetricsReporter")
    props.put("kafka.metrics.polling.interval.secs", "1")
    props.put("kafka.graphite.metrics.reporter.enabled", "true")
    props.put("kafka.graphite.metrics.jvm.enabled", "false")
    props.put("kafka.graphite.metrics.port", graphiteServer.port.toString)

    val graphiteMetricsReporter = new KafkaGraphiteMetricsReporter()
    graphiteMetricsReporter.init(new VerifiableProperties(props))

    val counter = Metrics.defaultRegistry().newCounter(new MetricName("test", "type", "counter"))
    counter.inc()
    Thread.sleep(2000)
    counter.inc()
    Thread.sleep(2000)

    assert(graphiteServer.content.toList.exists(item => item.contains("type.counter.count 1")))
    assert(graphiteServer.content.toList.exists(item => item.contains("type.counter.count 2")))

    graphiteMetricsReporter.stopReporter()
  }
  
  it should "exclude data" in {
    val props = new Properties
    props.put("kafka.metrics.reporters", "kafka.metrics.KafkaGraphiteMetricsReporter")
    props.put("kafka.metrics.polling.interval.secs", "1")
    props.put("kafka.graphite.metrics.reporter.enabled", "true")
    props.put("kafka.graphite.metrics.exclude", ".*test.*")
    props.put("kafka.graphite.metrics.port", graphiteServer.port.toString)

    val graphiteMetricsReporter = new KafkaGraphiteMetricsReporter()
    graphiteMetricsReporter.init(new VerifiableProperties(props))

    Metrics.defaultRegistry().newCounter(new MetricName("valid", "type", "counter")).inc()
    Metrics.defaultRegistry().newCounter(new MetricName("test", "type", "counter")).inc()

    Thread.sleep(2000)

    assert(graphiteServer.content.toList.exists(item => item.contains("valid.type")))
    assert(!graphiteServer.content.toList.exists(item => item.contains("test.type")))

    graphiteMetricsReporter.stopReporter()
  }

  it should "include data" in {
    val props = new Properties
    props.put("kafka.metrics.reporters", "kafka.metrics.KafkaGraphiteMetricsReporter")
    props.put("kafka.metrics.polling.interval.secs", "1")
    props.put("kafka.graphite.metrics.reporter.enabled", "true")
    props.put("kafka.graphite.metrics.include", ".*test.*")
    props.put("kafka.graphite.metrics.port", graphiteServer.port.toString)

    val graphiteMetricsReporter = new KafkaGraphiteMetricsReporter()
    graphiteMetricsReporter.init(new VerifiableProperties(props))

    Metrics.defaultRegistry().newCounter(new MetricName("invalid", "type", "counter")).inc()
    Metrics.defaultRegistry().newCounter(new MetricName("test", "type", "counter")).inc()

    Thread.sleep(2000)

    assert(!graphiteServer.content.toList.exists(item => item.contains("invalid.type")))
    assert(graphiteServer.content.toList.exists(item => item.contains("test.type")))

    graphiteMetricsReporter.stopReporter()
  }

  it should "include and exclude data in the same time" in {
    val props = new Properties
    props.put("kafka.metrics.reporters", "kafka.metrics.KafkaGraphiteMetricsReporter")
    props.put("kafka.metrics.polling.interval.secs", "1")
    props.put("kafka.graphite.metrics.reporter.enabled", "true")
    props.put("kafka.graphite.metrics.include", ".*valid.*")
    props.put("kafka.graphite.metrics.exclude", ".*invalid.*")
    props.put("kafka.graphite.metrics.port", graphiteServer.port.toString)

    val graphiteMetricsReporter = new KafkaGraphiteMetricsReporter()
    graphiteMetricsReporter.init(new VerifiableProperties(props))

    Metrics.defaultRegistry().newCounter(new MetricName("valid", "type", "counter")).inc()
    Metrics.defaultRegistry().newCounter(new MetricName("invalid", "type", "counter")).inc()
    Metrics.defaultRegistry().newCounter(new MetricName("test", "type", "counter")).inc()

    Thread.sleep(2000)

    assert(graphiteServer.content.toList.exists(item => item.contains("valid.type")))
    assert(!graphiteServer.content.toList.exists(item => item.contains("test.type")))
    assert(!graphiteServer.content.toList.exists(item => item.contains("invalid.type")))

    graphiteMetricsReporter.stopReporter()
  }

  it should "include jvm metrics by default" in {
    val props = new Properties
    props.put("kafka.metrics.reporters", "kafka.metrics.KafkaGraphiteMetricsReporter")
    props.put("kafka.metrics.polling.interval.secs", "1")
    props.put("kafka.graphite.metrics.reporter.enabled", "true")
    props.put("kafka.graphite.metrics.port", graphiteServer.port.toString)

    val graphiteMetricsReporter = new KafkaGraphiteMetricsReporter()
    graphiteMetricsReporter.init(new VerifiableProperties(props))

    Thread.sleep(2000)

    assert(graphiteServer.content.toList.exists(item => item.contains("jvm")))

    graphiteMetricsReporter.stopReporter()
  }

  it should "allow to disable jvm metrics" in {
    val props = new Properties
    props.put("kafka.metrics.reporters", "kafka.metrics.KafkaGraphiteMetricsReporter")
    props.put("kafka.metrics.polling.interval.secs", "1")
    props.put("kafka.graphite.metrics.reporter.enabled", "true")
    props.put("kafka.graphite.metrics.jvm.enabled", "false")
    props.put("kafka.graphite.metrics.port", graphiteServer.port.toString)

    val graphiteMetricsReporter = new KafkaGraphiteMetricsReporter()
    graphiteMetricsReporter.init(new VerifiableProperties(props))

    Thread.sleep(2000)

    assert(!graphiteServer.content.toList.exists(item => item.contains("jvm")))

    graphiteMetricsReporter.stopReporter()
  }
}