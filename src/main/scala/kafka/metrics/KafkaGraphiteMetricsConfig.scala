package kafka.metrics

import java.util.regex.Pattern

import kafka.utils.VerifiableProperties

class KafkaGraphiteMetricsConfig(props: VerifiableProperties) extends KafkaMetricsConfig(props) {

  val host = props.getString("kafka.graphite.metrics.host", "localhost")

  var port = props.getInt("kafka.graphite.metrics.port", 2003)

  var prefix = props.getString("kafka.graphite.metrics.prefix", "kafka")

  var enabled = props.getBoolean("kafka.graphite.metrics.reporter.enabled", default = false)

  var include = getPattern("kafka.graphite.metrics.include", null)

  var exclude = getPattern("kafka.graphite.metrics.exclude", null)

  private def getPattern(key: String, default: Pattern): Pattern = {
    if (!props.containsKey(key)) default
    else Pattern.compile(props.getProperty(key))
  }
}
