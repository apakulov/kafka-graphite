package kafka.metrics;

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.GraphiteReporter;
import com.yammer.metrics.reporting.SocketProvider;

import java.io.IOException;

public class KafkaGraphiteReporter extends GraphiteReporter {

    public KafkaGraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate,
                                 SocketProvider socketProvider, Clock clock) throws IOException {
        super(metricsRegistry, prefix, predicate, socketProvider, clock);
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Long epoch) throws IOException {
        super.processHistogram(name, histogram, epoch);
        System.out.println("clear histogram");
        histogram.clear();
    }
}
