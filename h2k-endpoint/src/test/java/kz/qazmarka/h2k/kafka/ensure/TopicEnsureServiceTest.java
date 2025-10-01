package kz.qazmarka.h2k.kafka.ensure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;
import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;

class TopicEnsureServiceTest {

    @Test
    @DisplayName("ensureTopic(): увеличивает партиции, если текущее число меньше заданного")
    void ensureTopic_increasesPartitionsWhenBelowTarget() {
        FakeAdmin admin = new FakeAdmin().withPartitions(1);
        TopicEnsureConfig cfg = TopicEnsureConfig.builder()
                .topicNameMaxLen(249)
                .topicSanitizer(UnaryOperator.identity())
                .topicPartitions(3)
                .topicReplication((short) 1)
                .topicConfigs(Collections.emptyMap())
                .ensureIncreasePartitions(true)
                .ensureDiffConfigs(false)
                .adminTimeoutMs(50L)
                .unknownBackoffMs(10L)
                .build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, new TopicEnsureState())) {
            svc.ensureTopic("orders");
        }

        assertTrue(admin.increaseCalled, "Ожидалось увеличение партиций до 3");
        assertEquals(3, admin.increaseNewCount, "Новая мощность должна совпадать с конфигом");
    }

    @Test
    @DisplayName("ensureTopic(): приводит конфиги темы diff-only")
    void ensureTopic_appliesConfigDiff() {
        Map<String, String> desired = Collections.singletonMap("retention.ms", "60000");
        FakeAdmin admin = new FakeAdmin()
                .withPartitions(3)
                .withConfig("retention.ms", "1000");
        TopicEnsureConfig cfg = TopicEnsureConfig.builder()
                .topicNameMaxLen(249)
                .topicSanitizer(UnaryOperator.identity())
                .topicPartitions(3)
                .topicReplication((short) 1)
                .topicConfigs(desired)
                .ensureIncreasePartitions(false)
                .ensureDiffConfigs(true)
                .adminTimeoutMs(50L)
                .unknownBackoffMs(10L)
                .build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, new TopicEnsureState())) {
            svc.ensureTopic("analytics");
        }

        assertTrue(admin.configUpdated, "Ожидалась передача ALTER CONFIG для retention.ms");
        assertEquals("60000", admin.lastConfigValue, "На брокер должен уйти diff с новым значением");
    }

    private static final class FakeAdmin implements KafkaTopicAdmin {
        private int partitions = 1;
        private Config topicConfig = emptyConfig();
        private boolean increaseCalled;
        private int increaseNewCount;
        private boolean configUpdated;
        private String lastConfigValue;

        FakeAdmin withPartitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        FakeAdmin withConfig(String key, String value) {
            this.topicConfig = new Config(Collections.singletonList(new ConfigEntry(key, value)));
            return this;
        }

        @Override
        public Map<String, KafkaFuture<TopicDescription>> describeTopics(Set<String> names) {
            Map<String, KafkaFuture<TopicDescription>> out = new LinkedHashMap<>();
            for (String n : names) {
                out.put(n, KafkaFuture.completedFuture(topicDescription(n, partitions)));
            }
            return out;
        }

        @Override
        public Map<String, KafkaFuture<Void>> createTopics(List<NewTopic> newTopics) {
            return Collections.emptyMap();
        }

        @Override
        public void createTopic(NewTopic topic, long timeoutMs) { /* no-op */ }

        @Override
        public void close(Duration timeout) { /* no-op */ }

        @Override
        public void increasePartitions(String topic, int newCount, long timeoutMs) {
            this.increaseCalled = true;
            this.increaseNewCount = newCount;
        }

        @Override
        public Map<ConfigResource, KafkaFuture<Config>> describeConfigs(Collection<ConfigResource> resources) {
            Map<ConfigResource, KafkaFuture<Config>> out = new LinkedHashMap<>(resources.size());
            for (ConfigResource r : resources) {
                out.put(r, KafkaFuture.completedFuture(topicConfig));
            }
            return out;
        }

        @Override
        public void incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops, long timeoutMs)
                throws InterruptedException, ExecutionException, TimeoutException {
            this.configUpdated = !ops.isEmpty();
            if (configUpdated) {
                AlterConfigOp op = ops.values().iterator().next().iterator().next();
                this.lastConfigValue = op.configEntry().value();
            }
        }

        private static TopicDescription topicDescription(String name, int partitions) {
            List<TopicPartitionInfo> infos = new ArrayList<>(partitions);
            Node leader = new Node(0, "localhost", 9092);
            List<Node> replicas = Collections.singletonList(leader);
            for (int p = 0; p < partitions; p++) {
                infos.add(new TopicPartitionInfo(p, leader, replicas, replicas));
            }
            return new TopicDescription(name, false, infos);
        }

        private static Config emptyConfig() {
            return new Config(Collections.<ConfigEntry>emptyList());
        }
    }
}
