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
package org.apache.kafka.common.metrics;

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.reporting.SocketProvider;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.GRAPHITE_HOST;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.GRAPHITE_PORT;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.INCLUDE;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.EXCLUDE;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.PREFIX;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.JVM_ENABLED;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.REPORTER_ENABLED;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.INTERVAL;

public class GraphiteReporter implements MetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(JmxReporter.class);
    private com.yammer.metrics.reporting.GraphiteReporter underlying;
    private boolean initialized;
    private GraphiteConfig config;

    @Override
    public synchronized void configure(final Map<String, ?> configs) {
        this.config = new GraphiteConfig(configs);
    }

    @Override
    public synchronized void init(List<KafkaMetric> metrics) {
        if (!initialized) {
            final String hostname = config.getString(GRAPHITE_HOST);
            final Integer port = config.getInt(GRAPHITE_PORT);
            final MetricPredicate metricPredicate = new GraphiteMetricPredicate(config.getString(INCLUDE), config.getString(EXCLUDE));
            final String prefix = config.getString(PREFIX);

            final SocketProvider socketProvider = new com.yammer.metrics.reporting.GraphiteReporter.DefaultSocketProvider(hostname, port);

            try {
                underlying = new com.yammer.metrics.reporting.GraphiteReporter(com.yammer.metrics.Metrics.defaultRegistry(),
                        prefix, metricPredicate, socketProvider, Clock.defaultClock());
                underlying.printVMMetrics = config.getBoolean(JVM_ENABLED);
            } catch (IOException e) {
                log.error("Enable to initialize Graphite Reporter", e);
            }

            if (config.getBoolean(REPORTER_ENABLED)) {
                underlying.start(config.getInt(INTERVAL), TimeUnit.SECONDS);
                initialized = true;
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
    }

    @Override
    public synchronized void close() {
        if (initialized) {
            underlying.shutdown();
            underlying = null;
        }
    }

    private static class GraphiteMetricPredicate implements MetricPredicate {
        private final Pattern include;
        private final Pattern exclude;

        public GraphiteMetricPredicate(String include, String exclude) {
            this.include = (null != include && !include.isEmpty()) ? Pattern.compile(include) : null;
            this.exclude = (null != exclude && !exclude.isEmpty()) ? Pattern.compile(exclude) : null;
        }

        @Override
        public boolean matches(MetricName name, Metric metric) {
            final String groupedMetricName = groupMetricName(name);
            if (null != include && !include.matcher(groupedMetricName).matches()) {
                return false;
            }
            if (null != exclude && exclude.matcher(groupedMetricName).matches()) {
                return false;
            }
            return true;
        }

        private String groupMetricName(MetricName name) {
            StringBuilder result = new StringBuilder().append(name.getGroup()).append('.').append(name.getType()).append('.');
            if (name.hasScope()) {
                result.append(name.getScope()).append('.');
            }
            return result.append(name.getName()).toString().replace(' ', '_');
        }
    }

    static class GraphiteConfig extends AbstractConfig {
        public static final String REPORTER_ENABLED = "kafka.graphite.metrics.reporter.enabled";
        public static final String GRAPHITE_HOST = "kafka.graphite.metrics.host";
        public static final String GRAPHITE_PORT = "kafka.graphite.metrics.port";
        public static final String PREFIX = "kafka.graphite.metrics.prefix";
        public static final String INCLUDE = "kafka.graphite.metrics.include";
        public static final String EXCLUDE = "kafka.graphite.metrics.exclude";
        public static final String JVM_ENABLED = "kafka.graphite.metrics.jvm.enabled";
        public static final String INTERVAL = "kafka.metrics.polling.interval.secs";

        private static final ConfigDef configDefinition = new ConfigDef()
                .define(REPORTER_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Enables actual plugin")
                .define(GRAPHITE_HOST, ConfigDef.Type.STRING, "localhost", ConfigDef.Importance.HIGH, "The graphite host to connect")
                .define(GRAPHITE_PORT, ConfigDef.Type.INT, 2003, ConfigDef.Importance.HIGH, "The port to connect")
                .define(PREFIX, ConfigDef.Type.STRING, "kafka", ConfigDef.Importance.MEDIUM, "The metric prefix that's sent with metric names")
                .define(INCLUDE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "A regular expression allowing explicitly include certain metrics")
                .define(EXCLUDE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "A regular expression allowing you to exclude certain metrics")
                .define(JVM_ENABLED, ConfigDef.Type.BOOLEAN, "true", ConfigDef.Importance.LOW, "Controls JVM metrics output")
                .define(INTERVAL, ConfigDef.Type.INT, "60", ConfigDef.Importance.MEDIUM, "Polling interval that will be used for all Kafka metrics");

        private GraphiteConfig(Map<?, ?> originals) {
            super(configDefinition, originals);
        }
    }
}
