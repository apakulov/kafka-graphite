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
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.isA;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GraphiteReporter.class})
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
    }

    @Test
    public void testCounterIncrement() throws Exception {
        Map<String, Object> configs = initializeConfigWithReporter();
        configs.put("kafka.graphite.metrics.jvm.enabled", "false");
        graphiteReporter.configure(configs);

        List<KafkaMetric> metrics = new ArrayList<>();
        final KafkaMetric metric = createMetric("test");
        final Count counter = (Count) metric.measurable();
        final MetricConfig config = metric.config();
        metrics.add(metric);
        graphiteReporter.init(metrics);


        counter.record(config, 1, System.currentTimeMillis());
        Thread.sleep(2000);
        counter.record(config, 2, System.currentTimeMillis());
        Thread.sleep(2000);

        assertThat(graphiteServer.content, hasItem(containsString("group.topic.test 1.00")));
        assertThat(graphiteServer.content, hasItem(containsString("group.topic.test 2.00")));
        graphiteReporter.close();
    }

    @Test
    public void testExcludeData() throws Exception {
        Map<String, Object> configs = initializeConfigWithReporter();
        configs.put("kafka.graphite.metrics.exclude", ".*test.*");
        configs.put("kafka.graphite.metrics.jvm.enabled", "false");
        graphiteReporter.configure(configs);

        List<KafkaMetric> metrics = new ArrayList<>();
        metrics.add(createMetric("valid"));
        metrics.add(createMetric("test"));
        graphiteReporter.init(metrics);

        Thread.sleep(2000);

        assertThat(graphiteServer.content, hasItem(containsString("group.topic.valid")));
        assertThat(graphiteServer.content, not(hasItem(containsString("group.topic.test"))));
        graphiteReporter.close();
    }

    @Test
    public void textIncludeData() throws Exception {
        Map<String, Object> configs = initializeConfigWithReporter();
        configs.put("kafka.graphite.metrics.include", ".*test.*");
        graphiteReporter.configure(configs);

        List<KafkaMetric> metrics = new ArrayList<>();
        metrics.add(createMetric("valid"));
        metrics.add(createMetric("test"));
        graphiteReporter.init(metrics);

        Thread.sleep(2000);

        assertThat(graphiteServer.content, not(hasItem(containsString("group.topic.invalid"))));
        assertThat(graphiteServer.content, hasItem(containsString("group.topic.test")));
        graphiteReporter.close();
    }

    @Test
    public void testExcludeIncludeData() throws Exception {
        Map<String, Object> configs = initializeConfigWithReporter();
        configs.put("kafka.graphite.metrics.include", ".*valid.*");
        configs.put("kafka.graphite.metrics.exclude", ".*invalid.*");
        graphiteReporter.configure(configs);

        List<KafkaMetric> metrics = new ArrayList<>();
        metrics.add(createMetric("valid"));
        metrics.add(createMetric("invalid"));
        metrics.add(createMetric("test"));
        graphiteReporter.init(metrics);

        Thread.sleep(2000);

        assertThat(graphiteServer.content, hasItem(containsString("group.topic.valid")));
        assertThat(graphiteServer.content, not(hasItem(containsString("group.topic.test"))));
        assertThat(graphiteServer.content, not(hasItem(containsString("group.topic.invalid"))));
        graphiteReporter.close();
    }

    @Test
    public void testRemoveMetric() throws Exception {
        Map<String, Object> configs = initializeConfigWithReporter();
        graphiteReporter.configure(configs);

        final KafkaMetric metricToRemove = createMetric("valid-to-remove");
        List<KafkaMetric> metrics = Arrays.asList(createMetric("valid"), metricToRemove);
        graphiteReporter.init(metrics);

        Thread.sleep(2000);

        assertThat(graphiteServer.content, hasItem(containsString("group.topic.valid")));
        assertThat(graphiteServer.content, hasItem(containsString("group.topic.valid-to-remove")));

        graphiteReporter.metricRemoval(metricToRemove);
        graphiteServer.content.clear();
        Thread.sleep(2000);

        assertThat(graphiteServer.content, hasItem(containsString("group.topic.valid")));
        assertThat(graphiteServer.content, not(hasItem(containsString("group.topic.valid-to-remove"))));
        graphiteReporter.close();
    }

    @Test
    public void testConcurrentModification() throws InterruptedException {
        Map<String, Object> configs = initializeConfigWithReporter();
        graphiteReporter.configure(configs);
        final CountDownLatch errorLatch = new CountDownLatch(1);

        // Stub setup.
        Logger logger = mock(Logger.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                errorLatch.countDown();
                return null;
            }
        }).when(logger).warn(anyString(), isA(ConcurrentModificationException.class));
        Whitebox.setInternalState(GraphiteReporter.class, "log", logger);

        // Metric setup and start reporting to Graphite
        final KafkaMetric metricToRemove = createMetric("valid-to-remove");
        List<KafkaMetric> metrics = new ArrayList<>();
        metrics.add(metricToRemove);
        for (int i = 0; i < 10; i++) {
            metrics.add(createMetric("valid" + i));
        }
        graphiteReporter.init(metrics);

        // Try to cause a CME.
        ExecutorService exceptionCause = Executors.newFixedThreadPool(1);
        CMERunnable task = new CMERunnable(metricToRemove);
        exceptionCause.submit(task);

        boolean hasError = errorLatch.await(2, TimeUnit.SECONDS);
        task.isRunning = false;
        exceptionCause.shutdown();
        exceptionCause.awaitTermination(1, TimeUnit.SECONDS);

        assertThat(hasError, is(false));
    }

    @Test
    public void testInitFailure() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("metric.reporters", "org.apache.kafka.common.metrics.GraphiteReporter");
        configs.put("kafka.metrics.polling.interval.secs", "1");
        configs.put("kafka.graphite.metrics.reporter.enabled", "true");
        configs.put("kafka.graphite.metrics.host", "localhost");
        configs.put("kafka.graphite.metrics.port", "0");

        graphiteReporter.configure(configs);
        graphiteReporter.init(Collections.<KafkaMetric>emptyList());
        graphiteReporter.close();
    }

    @Test
    public void testClose() throws InterruptedException {
        Map<String, Object> configs = initializeConfigWithReporter();
        graphiteReporter.configure(configs);

        ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        Future mockFuture = mock(Future.class);
        when(mockExecutor.submit(graphiteReporter)).thenReturn(mockFuture);
        when(mockExecutor.awaitTermination(20, TimeUnit.SECONDS)).thenReturn(true);

        Whitebox.setInternalState(graphiteReporter, "executor", mockExecutor);
        graphiteReporter.close();

        verify(mockExecutor).shutdown();
        verify(mockExecutor).submit(graphiteReporter);
        verify(mockExecutor).awaitTermination(20, TimeUnit.SECONDS);
    }

    private KafkaMetric createMetric(final String topicName) {
        final Map<String, String> tags = new HashMap<>();
        tags.put("client-id", "topic");
        final MetricName group = new Metrics().metricName(topicName, "group", tags);
        return new KafkaMetric(new Object(), group, new Count(), new MetricConfig(), new SystemTime());
    }

    private Map<String, Object> initializeConfigWithReporter() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("metric.reporters", "org.apache.kafka.common.metrics.GraphiteReporter");
        configs.put("kafka.metrics.polling.interval.secs", "1");
        configs.put("kafka.graphite.metrics.reporter.enabled", "true");
        configs.put("kafka.graphite.metrics.host", "localhost");
        configs.put("kafka.graphite.metrics.port", String.valueOf(graphiteServer.port));
        return configs;
    }

    private static class GraphiteMockServer extends Thread {
        private List<String> content = new ArrayList<>();
        private Socket socket;
        private ServerSocket server;
        private Integer port;

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

    private class CMERunnable implements Runnable {

        private final KafkaMetric metricToRemove;
        volatile boolean isRunning;

        public CMERunnable(KafkaMetric metricToRemove) {
            this.metricToRemove = metricToRemove;
            isRunning = true;
        }

        @Override
        public void run() {
            while (isRunning) {
                graphiteReporter.metricRemoval(metricToRemove);
                graphiteReporter.metricChange(metricToRemove);
            }
        }
    }
}