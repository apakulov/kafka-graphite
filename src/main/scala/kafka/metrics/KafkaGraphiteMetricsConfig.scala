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
