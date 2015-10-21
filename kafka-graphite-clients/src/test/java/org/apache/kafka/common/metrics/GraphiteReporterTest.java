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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class GraphiteReporterTest {
    private GraphiteMockServer graphiteServer;
    private GraphiteReporter graphiteReporter;

    @Before
    public void setUp() {
        graphiteServer = new GraphiteMockServer();
        graphiteServer.start();
        graphiteReporter = new GraphiteReporter();
    }

    @After
    public void tearDown() {
        graphiteServer.close();
        graphiteReporter.close();
    }

    @Test
    public void testExcludeData() throws Exception {
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("metric.reporters", "org.apache.kafka.common.metrics.GraphiteReporter");
        configs.put("kafka.metrics.polling.interval.secs", "1");
        configs.put("kafka.graphite.metrics.reporter.enabled", "true");
        configs.put("kafka.graphite.metrics.host", "localhost");
        configs.put("kafka.graphite.metrics.exclude", ".*test.*");
        configs.put("kafka.graphite.metrics.jvm.enabled", "false");
        configs.put("kafka.graphite.metrics.port", String.valueOf(graphiteServer.port));
        graphiteReporter.configure(configs);

        List<KafkaMetric> metrics = new ArrayList<KafkaMetric>();
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("client-id", "topic");
        metrics.add(new KafkaMetric(new Object(), new MetricName("type", "valid", tags), new Count(), new MetricConfig(), new SystemTime()));
        metrics.add(new KafkaMetric(new Object(), new MetricName("type", "test", tags), new Count(), new MetricConfig(), new SystemTime()));
        graphiteReporter.init(metrics);

        Thread.sleep(2000);

        assertThat(graphiteServer.content, hasItem(containsString("valid.topic.type")));
        assertThat(graphiteServer.content, not(hasItem(containsString("test.topic.type"))));
    }

    @Test
    public void textIncludeData() throws Exception {
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("metric.reporters", "org.apache.kafka.common.metrics.GraphiteReporter");
        configs.put("kafka.metrics.polling.interval.secs", "1");
        configs.put("kafka.graphite.metrics.reporter.enabled", "true");
        configs.put("kafka.graphite.metrics.host", "localhost");
        configs.put("kafka.graphite.metrics.include", ".*test.*");
        configs.put("kafka.graphite.metrics.port", String.valueOf(graphiteServer.port));
        graphiteReporter.configure(configs);

        List<KafkaMetric> metrics = new ArrayList<KafkaMetric>();
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("client-id", "topic");
        metrics.add(new KafkaMetric(new Object(), new MetricName("type", "valid", tags), new Count(), new MetricConfig(), new SystemTime()));
        metrics.add(new KafkaMetric(new Object(), new MetricName("type", "test", tags), new Count(), new MetricConfig(), new SystemTime()));
        graphiteReporter.init(metrics);

        Thread.sleep(2000);

        assertThat(graphiteServer.content, not(hasItem(containsString("invalid.topic.type"))));
        assertThat(graphiteServer.content, hasItem(containsString("test.topic.type")));
    }

    @Test
    public void testExcludeIncludeData() throws Exception {
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("metric.reporters", "org.apache.kafka.common.metrics.GraphiteReporter");
        configs.put("kafka.metrics.polling.interval.secs", "1");
        configs.put("kafka.graphite.metrics.reporter.enabled", "true");
        configs.put("kafka.graphite.metrics.host", "localhost");
        configs.put("kafka.graphite.metrics.include", ".*valid.*");
        configs.put("kafka.graphite.metrics.exclude", ".*invalid.*");
        configs.put("kafka.graphite.metrics.port", String.valueOf(graphiteServer.port));
        graphiteReporter.configure(configs);

        List<KafkaMetric> metrics = new ArrayList<KafkaMetric>();
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("client-id", "topic");
        metrics.add(new KafkaMetric(new Object(), new MetricName("type", "valid", tags), new Count(), new MetricConfig(), new SystemTime()));
        metrics.add(new KafkaMetric(new Object(), new MetricName("type", "invalid", tags), new Count(), new MetricConfig(), new SystemTime()));
        metrics.add(new KafkaMetric(new Object(), new MetricName("type", "test", tags), new Count(), new MetricConfig(), new SystemTime()));
        graphiteReporter.init(metrics);

        Thread.sleep(2000);

        assertThat(graphiteServer.content, hasItem(containsString("valid.topic.type")));
        assertThat(graphiteServer.content, not(hasItem(containsString("test.topic.type"))));
        assertThat(graphiteServer.content, not(hasItem(containsString("invalid.topic.type"))));
    }

    @Test
    public void testInitFailure() {
        final Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("metric.reporters", "org.apache.kafka.common.metrics.GraphiteReporter");
        configs.put("kafka.metrics.polling.interval.secs", "1");
        configs.put("kafka.graphite.metrics.reporter.enabled", "true");
        configs.put("kafka.graphite.metrics.host", "localhost");
        configs.put("kafka.graphite.metrics.port", "0");

        graphiteReporter.configure(configs);
        graphiteReporter.init(Collections.<KafkaMetric>emptyList());
    }

    private static class GraphiteMockServer extends Thread {
        private List<String> content = new ArrayList<String>();
        private Socket socket;
        private ServerSocket server;
        protected Integer port;

        public GraphiteMockServer() {
            try {
                this.server = new ServerSocket(new Random().nextInt(65000));
            } catch (IOException e) {
                throw new RuntimeException("Unable to start ServerSocket", e);
            }
            this.port = server.getLocalPort();
        }

        @Override
        public void run() {
            while (!server.isClosed()) {
                try {
                    socket = server.accept();
                    final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String str;
                    while ((str = bufferedReader.readLine()) != null) {
                        content.add(str);
                    }
                } catch (IOException e) {
                    // Bye-bye, I'm dying
                }
            }
        }

        public void close() {
            try {
                server.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
