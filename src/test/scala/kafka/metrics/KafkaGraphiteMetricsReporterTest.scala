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
        case e: SocketException => "Socket closed?"
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
}