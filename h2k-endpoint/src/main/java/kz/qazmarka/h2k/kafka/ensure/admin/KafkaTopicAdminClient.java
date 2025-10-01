package kz.qazmarka.h2k.kafka.ensure.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Обёртка над {@link AdminClient}, реализующая {@link KafkaTopicAdmin} без дополнительной логики.
 */
public final class KafkaTopicAdminClient implements KafkaTopicAdmin {

    private final AdminClient delegate;

    public KafkaTopicAdminClient(AdminClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<String, KafkaFuture<TopicDescription>> describeTopics(java.util.Set<String> names) {
        return delegate.describeTopics(names).values();
    }

    @Override
    public Map<String, KafkaFuture<Void>> createTopics(List<NewTopic> newTopics) {
        return delegate.createTopics(newTopics).values();
    }

    @Override
    public void createTopic(NewTopic topic, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        delegate.createTopics(java.util.Collections.singleton(topic))
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    @Override
    public void increasePartitions(String topic,
                                   int newCount,
                                   long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        delegate.createPartitions(
                        java.util.Collections.singletonMap(topic, NewPartitions.increaseTo(newCount)))
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public Map<ConfigResource, KafkaFuture<Config>> describeConfigs(Collection<ConfigResource> resources) {
        return delegate.describeConfigs(resources).values();
    }

    @Override
    public void incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops,
                                        long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        delegate.incrementalAlterConfigs(ops)
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
    }
}
