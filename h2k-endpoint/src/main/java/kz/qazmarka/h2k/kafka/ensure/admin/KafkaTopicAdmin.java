package kz.qazmarka.h2k.kafka.ensure.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Минимальный контракт для работы с Kafka AdminClient в ensure-процессе.
 *
 * Интерфейс преднамеренно лишён зависимостей от конкретной реализации AdminClient, что упрощает unit-тесты.
 */
public interface KafkaTopicAdmin {

    /** Запрашивает описания тем и возвращает map future'ов по именам. */
    Map<String, KafkaFuture<TopicDescription>> describeTopics(Set<String> names);

    /** Инициирует batch create и возвращает future'ы по именам. */
    Map<String, KafkaFuture<Void>> createTopics(List<NewTopic> newTopics);

    /** Создаёт одну тему с ожиданием до {@code timeoutMs}. */
    void createTopic(NewTopic topic, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException;

    void close(Duration timeout);

    /** Увеличивает число партиций темы, либо бросает исключение при неуспехе. */
    void increasePartitions(String topic, int newCount, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException;

    /** Возвращает текущие конфиги указанных ресурсов. */
    Map<ConfigResource, KafkaFuture<Config>> describeConfigs(Collection<ConfigResource> resources);

    /** Применяет дифф-конфиги (incremental alter) и ждёт завершения. */
    void incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops,
                                 long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException;
}
