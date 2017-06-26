/**
 * Copyright 2017 Alexander Pakulov
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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.GRAPHITE_HOST;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.GRAPHITE_PORT;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.INCLUDE;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.EXCLUDE;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.PREFIX;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.REPORTER_ENABLED;
import static org.apache.kafka.common.metrics.GraphiteReporter.GraphiteConfig.INTERVAL;

public class GraphiteReporter implements MetricsReporter, Runnable {
    private static final Logger log = LoggerFactory.getLogger(GraphiteReporter.class);

    private List<KafkaMetric> metricList = Collections.synchronizedList(new ArrayList<KafkaMetric>());
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
    private GraphiteConfig config;

    private String prefix;
    private String hostname;
    private int port;
    private Pattern include;
    private Pattern exclude;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new GraphiteConfig(configs);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        this.hostname = config.getString(GRAPHITE_HOST);
        this.port = config.getInt(GRAPHITE_PORT);
        this.prefix = config.getString(PREFIX);

        final String includeRegex = config.getString(INCLUDE);
        final String excludeRegex = config.getString(EXCLUDE);
        this.include = includeRegex != null && !includeRegex.isEmpty() ? Pattern.compile(includeRegex) : null;
        this.exclude = excludeRegex != null && !excludeRegex.isEmpty() ? Pattern.compile(excludeRegex) : null;

        if (config.getBoolean(REPORTER_ENABLED)) {
            final int interval = config.getInt(INTERVAL);

            for (final KafkaMetric metric : metrics) {
                metricList.add(metric);
            }
            log.info("Configuring Kafka Graphite Reporter with host={}, port={}, prefix={} and include={}, exclude={}",
                    hostname, port, prefix, includeRegex, excludeRegex);
            executor.scheduleAtFixedRate(this, interval, interval, TimeUnit.SECONDS);
        }
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        metricList.add(metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        metricList.remove(metric);
    }

    @Override
    public void close() {
        if (config.getBoolean(REPORTER_ENABLED)) {
            executor.submit(this);
        }
        executor.shutdown();
        try {
            // A 20 second timeout should be enough to finish the remaining tasks.
            if (executor.awaitTermination(20, TimeUnit.SECONDS)) {
                log.debug("Executor was shut down successfully.");
            } else {
                log.error("Timed out before executor was shut down! It's possible some metrics data were not sent out!");
            }
        } catch (InterruptedException e) {
            log.error("Unable to shutdown executor gracefully", e);
        }
    }

    /** This run method can be called for two purposes:
     *    - As a scheduled task, see scheduleAtFixedRate
     *    - As a final task when close() is called
     *  However, since the size of the ScheduledExecutorService is 1, there's no need to synchronize it.
     */
    @Override
    public void run() {
        Socket socket = null;
        Writer writer = null;
        try {
            socket = new Socket(hostname, port);
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            final long timestamp = System.currentTimeMillis() / 1000;

            for (KafkaMetric metric : metricList) {
                double value = metric.value();
                // DO NOT send an invalid value to graphite
                if (Double.NEGATIVE_INFINITY == value || Double.isNaN(value)) {
                    continue;
                }
                final String name = sanitizeName(metric.metricName());
                if (null != include && !include.matcher(name).matches()) {
                    continue;
                }
                if (null != exclude && exclude.matcher(name).matches()) {
                    continue;
                }

                if (prefix != null && !prefix.isEmpty()) {
                    writer.write(config.getString(PREFIX));
                    writer.write('.');
                }
                writer.write("kafka.");
                writer.write(name);
                writer.write(' ');
                writer.write(String.format(Locale.US, "%2.2f", value));
                writer.write(' ');
                writer.write(Long.toString(timestamp));
                writer.write('\n');
                writer.flush();
            }
        } catch (Exception e) {
            log.warn("Error writing to Graphite", e);
            if (writer != null) {
                try {
                    writer.flush();
                } catch (IOException e1) {
                    log.error("Error while flushing writer:", e1);
                }
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    log.error("Error while closing socket:", e);
                }
            }
        }
    }

    String sanitizeName(MetricName name) {
        StringBuilder result = new StringBuilder().append(name.group()).append('.');
        for (Map.Entry<String, String> tag : name.tags().entrySet()) {
            result.append(tag.getValue().replace(".", "_")).append('.');
        }
        return result.append(name.name()).toString().replace(' ', '_');
    }

    public static class GraphiteConfig extends AbstractConfig {
        public static final String REPORTER_ENABLED = "kafka.graphite.metrics.reporter.enabled";
        public static final String GRAPHITE_HOST = "kafka.graphite.metrics.host";
        public static final String GRAPHITE_PORT = "kafka.graphite.metrics.port";
        public static final String PREFIX = "kafka.graphite.metrics.prefix";
        public static final String INCLUDE = "kafka.graphite.metrics.include";
        public static final String EXCLUDE = "kafka.graphite.metrics.exclude";
        public static final String INTERVAL = "kafka.metrics.polling.interval.secs";

        private static final ConfigDef configDefinition = new ConfigDef()
                .define(REPORTER_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Enables actual plugin")
                .define(GRAPHITE_HOST, ConfigDef.Type.STRING, "localhost", ConfigDef.Importance.HIGH, "The graphite host to connect")
                .define(GRAPHITE_PORT, ConfigDef.Type.INT, 2003, ConfigDef.Importance.HIGH, "The port to connect")
                .define(PREFIX, ConfigDef.Type.STRING, "kafka", ConfigDef.Importance.MEDIUM, "The metric prefix that's sent with metric names")
                .define(INCLUDE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "A regular expression allowing explicitly include certain metrics")
                .define(EXCLUDE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "A regular expression allowing you to exclude certain metrics")
                .define(INTERVAL, ConfigDef.Type.INT, "60", ConfigDef.Importance.MEDIUM, "Polling interval that will be used for all Kafka metrics");

        private GraphiteConfig(Map<?, ?> originals) {
            super(configDefinition, originals);
        }
    }
}