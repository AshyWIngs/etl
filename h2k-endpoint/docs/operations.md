

# Операции эксплуатации

## Подготовка таблиц

Перед настройкой репликации убедитесь, что нужные Column Family (CF) активированы для репликации.

- Для каждой таблицы, которую требуется реплицировать (например, `HISTORY`, `RECEIPT`, `DOCUMENTS`), выполните:
    1. **Отключите таблицу** (disable):
        ```
        disable 'HISTORY'
        ```
    2. **Измените (alter) список реплицируемых CF**:
        ```
        alter 'HISTORY', {NAME => 'replication_scope', VERSIONS => 1}
        ```
        или:
        ```
        alter 'HISTORY', {NAME => 'cf_name', REPLICATION_SCOPE => 1}
        ```
        (укажите нужный CF и выставьте `REPLICATION_SCOPE` в 1)
    3. **Включите таблицу** (enable):
        ```
        enable 'HISTORY'
        ```
    4. Повторите для других таблиц (`RECEIPT`, `DOCUMENTS` и т.д.).

**Примечание:** для некоторых таблиц может потребоваться изменить только определённые CF.

## Добавление peer

Для подключения нового peer используйте команду:
```ruby
add_peer 'peer1',
  { 'ENDPOINT_CLASSNAME' => 'kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint',
    'CONFIG' => {
      'h2k.kafka.bootstrap.servers' => 'host1:9092,host2:9092',
      'h2k.topic.pattern'           => '${table}',
      'h2k.cf.list'                 => 'd',
      'h2k.decode.mode'             => 'json-phoenix',
      'h2k.schema.path'             => '/opt/hbase-default-current/conf/schema.json'
    }
  }
```
- `<peer_id>` — уникальный идентификатор peer.
- `ENDPOINT_CLASSNAME` — всегда: `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`
- `CONFIG` — карта с ключами `h2k.*` (см. примеры профилей).

**Примеры профилей:**
- `conf/add_peer_shell_fast.txt` — профиль FAST (низкая задержка, минимальные гарантии)
- `conf/add_peer_shell_balanced.txt` — профиль BALANCED (компромисс между скоростью и надёжностью)
- `conf/add_peer_shell_reliable.txt` — профиль RELIABLE (максимальная надёжность, выше задержки)

## Управление peer

- **Список всех peers:**
    ```
    list_peers
    ```
- **Показать таблицы и CF, реплицируемые через peer:**
    ```
    show_peer_tableCFs '<peer_id>'
    ```
- **Включить peer:**
    ```
    enable_peer '<peer_id>'
    ```
- **Отключить peer:**
    ```
    disable_peer '<peer_id>'
    ```
- **Удалить peer:**
    ```
    remove_peer '<peer_id>'
    ```

## Проверка статуса

- **Проверить статус репликации:**
    ```
    status 'replication'
    ```
    Вывод покажет состояние каждого peer, очереди, задержки, ошибки.

- **Интерпретация статуса:**
    - `PeerState=ENABLED` — репликация активна.
    - `SizeOfLogQueue` — размер очереди; если растёт, значит есть задержки.
    - Ошибки и stacktrace — ищите строки с ERROR.

## Быстрая диагностика

- Все логи пишутся в `journald` (systemd).
    - Для просмотра последних сообщений:
        ```
        journalctl -u h2k-endpoint.service -n 200
        ```
- Для расширенной отладки включите DEBUG для пакета:
    - В конфиге log4j или через переменную окружения установите уровень DEBUG для `kz.qazmarka.h2k.endpoint`.
    - Пример для log4j:
        ```
        log4j.logger.kz.qazmarka.h2k.endpoint=DEBUG
        ```

## Подсказки по тюнингу

- Для снижения задержек:
    - Уменьшите `batch.size` (например, до 16384 или ниже).
    - Уменьшите `linger.ms` (например, до 5-10 мс).
    - Увеличьте `acks` до 1 или 0 для ускорения, но потеряете надёжность.
- Для повышения надёжности:
    - Установите `acks=all`.
    - Увеличьте `retries` (например, до 10).
    - Используйте профиль RELIABLE.
- Для сбалансированной работы:
    - Используйте профиль BALANCED.
    - Проверьте значения параметров в профиле и при необходимости скорректируйте под свою нагрузку.

**Важно:** Для детальной настройки параметров используйте примеры профилей в `conf/add_peer_shell_*.txt`.

---

## Связанные документы

- [config.md](config.md) — параметры конфигурации endpoint
- [peer-profiles.md](peer-profiles.md) — описание профилей репликации
- [hbase.md](hbase.md) — подробности по работе с HBase
- [troubleshooting.md](troubleshooting.md) — диагностика и устранение проблем