# LogBroker

`LogBroker` — это учебный пет-проект на Go: одиночный брокер сообщений, в котором я пытался повторить и изучить базовые механики Kafka.

Цель проекта — не повторить Kafka целиком и не сделать совместимую замену Kafka, а реализовать своими руками и понять ее ключевые идеи на упрощенной модели.

- `topic` и `partition`
- журнал на диске, в который данные только дописываются
- `offset` и чтение с произвольного `offset`
- сегменты `.log` и `.index`
- сохраненные `offset` для consumer groups
- восстановление после перезапуска
- gRPC-интерфейс для сценариев producer/consumer
- HTTP-интерфейс для административных операций

Проект уже находится в хорошем демонстрационном состоянии: его можно запускать локально, прогонять через CLI и `grpcurl`, а также проверять автоматическими тестами.

## Что Есть Сейчас

На текущем этапе реализовано:

- надежное хранение журналов `partition` на диске
- хранение данных по сегментам в файлах `.log` и `.index`
- чтение записей по `offset` через индекс
- восстановление индексов при повторном открытии
- восстановление не только активного, но и старых сегментов
- очистка старых сегментов по размеру
- хранилище сведений о `topic`
- хранилище сохраненных `offset` для consumer groups
- сервисный слой брокера
- gRPC методы:
  - `CreateTopic`
  - `Produce`
  - `Fetch`
  - `CommitOffset`
  - `GetCommittedOffset`
- HTTP-методы для административных операций:
  - `GET /health`
  - `GET /topics`
  - `POST /topics`
- CLI для демонстрационных сценариев
- ограниченное чтение батчами:
  - `max_records` в `FetchRequest`
  - `next_offset` в `FetchResponse`
- защита от гонок для:
  - кэша открытых `PartitionLog`
  - самих `PartitionLog`
  - хранилища сведений о `topic`
  - хранилища сохраненных `offset`
- более безопасное сохранение метаданных и `offset` через `tmp + rename`
- проверка имени `topic`
- автоматические тесты для слоя хранения, брокера, gRPC и HTTP

## Чего в проекте нет

Это не полноценная замена Kafka. В проекте сознательно **нет**:

- репликации
- выбора лидера
- совместимости с сетевым протоколом Kafka
- транзакций
- семантики exactly-once
- протокола перераспределения consumer groups
- фонового уплотнения данных
- очистки по времени
- потокового или долгого чтения с ожиданием

То есть это не распределенный клон Kafka и не совместимая реализация ее протокола, а локальный брокер, через который я изучал ключевые идеи Kafka и систем обмена сообщениями на основе журнала.

## Архитектура

Структура проекта:

- [cmd/broker/main.go](cmd/broker/main.go) — точка входа брокера
- [cmd/cli/main.go](cmd/cli/main.go) — CLI для демонстрации
- [internal/storage](internal/storage) — сегменты, индекс, очистка, восстановление
- [internal/metadata](internal/metadata) — сведения о `topic`
- [internal/group](internal/group) — сохраненные `offset` групп
- [internal/broker](internal/broker) — координация и сервисный слой
- [internal/api/grpc](internal/api/grpc) — gRPC-обработчики
- [internal/api/http](internal/api/http) — HTTP-обработчики для административных операций
- [api/proto/broker.proto](api/proto/broker.proto) — protobuf-контракт

Общая схема:

```text
CLI / Producer / Consumer
        ->
   gRPC / HTTP API
        ->
    broker.Service
        ->
 metadata / group store / partition logs
        ->
          disk
```

Иерархия данных:

```text
Topic
  -> Partition
       -> PartitionLog
            -> Segment
                 -> Record
```

## Формат Данных На Диске

Пример структуры каталогов:

```text
data/
  metadata/
    topics.json
  groups/
    offsets.json
  topics/
    orders/
      0/
        00000000000000000000.log
        00000000000000000000.index
        00000000000000000042.log
        00000000000000000042.index
```

Где:

- `topics.json` — сведения о `topic`
- `offsets.json` — сохраненные `offset` consumer groups
- `*.log` — сегменты с данными
- `*.index` — индекс для быстрого чтения по offset

Источник истины — `.log`. Индексы могут быть пересобраны при восстановлении.

## Требования

Для локального запуска понадобятся:

- Go
- `grpcurl` — опционально, если хочешь тестировать gRPC руками

Версия Go в модуле:

```text
go 1.24.0
```

## Быстрый Старт

Запуск брокера с настройками по умолчанию:

```bash
go run ./cmd/broker
```

Запуск брокера с конфигурационным файлом:

```bash
go run ./cmd/broker -config broker.example.json
```

Пример конфига:

- [broker.example.json](broker.example.json)

Адреса по умолчанию:

- gRPC: `localhost:9090`
- HTTP: `http://localhost:8080`

Оба адреса можно менять через конфигурационный файл.

## CLI Сценарий

Короткий end-to-end сценарий:

1. подними broker
2. создай `topic`
3. запиши несколько сообщений
4. прочитай их батчами
5. сохрани consumer offset
6. прочитай сохраненный offset

Broker в одном терминале:

```bash
go run ./cmd/broker -config broker.example.json
```

CLI во втором терминале:

```bash
go run ./cmd/cli health
go run ./cmd/cli create-topic -name orders-demo -partitions 1
go run ./cmd/cli produce -topic orders-demo -partition 0 -key id-1 -value hello
go run ./cmd/cli produce -topic orders-demo -partition 0 -key id-2 -value world
go run ./cmd/cli produce -topic orders-demo -partition 0 -key id-3 -value again
go run ./cmd/cli consume -topic orders-demo -partition 0 -offset 0 -max-records 2
go run ./cmd/cli consume -topic orders-demo -partition 0 -offset 2 -max-records 2
go run ./cmd/cli commit-offset -group demo -topic orders-demo -partition 0 -offset 3
go run ./cmd/cli get-offset -group demo -topic orders-demo -partition 0
```

Ожидаемый смысл результата:

- первый `consume` вернет две записи и `next_offset=2`
- второй `consume` дочитает последнюю запись и вернет `next_offset=3`
- `commit-offset` сохранит прогресс группы `demo` на offset `3`
- `get-offset` вернет сохраненный offset `3`

Проверка health:

```bash
go run ./cmd/cli health
```

Создание topic:

```bash
go run ./cmd/cli create-topic -name orders-demo -partitions 1
```

Запись сообщений:

```bash
go run ./cmd/cli produce -topic orders-demo -partition 0 -key id-1 -value hello
go run ./cmd/cli produce -topic orders-demo -partition 0 -key id-2 -value world
go run ./cmd/cli produce -topic orders-demo -partition 0 -key id-3 -value again
```

Чтение батча с `offset`:

```bash
go run ./cmd/cli consume -topic orders-demo -partition 0 -offset 0 -max-records 2
```

CLI выведет записи и `next_offset`, который можно использовать в следующем запросе.

Продолжение чтения:

```bash
go run ./cmd/cli consume -topic orders-demo -partition 0 -offset 2 -max-records 2
```

Сохранение `offset` группы:

```bash
go run ./cmd/cli commit-offset -group demo -topic orders-demo -partition 0 -offset 3
```

Чтение сохраненного `offset`:

```bash
go run ./cmd/cli get-offset -group demo -topic orders-demo -partition 0
```

Глобальные флаги CLI:

- `-grpc-addr` — по умолчанию `localhost:9090`
- `-http-addr` — по умолчанию `http://localhost:8080`
- `-timeout` — по умолчанию `5s`

## gRPC Примеры Через grpcurl

Создать topic:

```bash
grpcurl -plaintext \
  -import-path api/proto \
  -proto broker.proto \
  -d '{"name":"orders-grpc","partitions":1}' \
  localhost:9090 \
  broker.v1.BrokerService/CreateTopic
```

Записать сообщение:

```bash
grpcurl -plaintext \
  -import-path api/proto \
  -proto broker.proto \
  -d '{"topic":"orders-grpc","partition":0,"key":"aWQtMQ==","value":"aGVsbG8="}' \
  localhost:9090 \
  broker.v1.BrokerService/Produce
```

Прочитать батч:

```bash
grpcurl -plaintext \
  -import-path api/proto \
  -proto broker.proto \
  -d '{"topic":"orders-grpc","partition":0,"offset":0,"max_records":2}' \
  localhost:9090 \
  broker.v1.BrokerService/Fetch
```

Ответ будет содержать:

- `records`
- `next_offset`

Сохранить `offset` группы:

```bash
grpcurl -plaintext \
  -import-path api/proto \
  -proto broker.proto \
  -d '{"group":"demo","topic":"orders-grpc","partition":0,"offset":1}' \
  localhost:9090 \
  broker.v1.BrokerService/CommitOffset
```

Прочитать сохраненный `offset`:

```bash
grpcurl -plaintext \
  -import-path api/proto \
  -proto broker.proto \
  -d '{"group":"demo","topic":"orders-grpc","partition":0}' \
  localhost:9090 \
  broker.v1.BrokerService/GetCommittedOffset
```

## HTTP Admin API

Проверка health:

```bash
curl http://localhost:8080/health
```

Получить список `topic`:

```bash
curl http://localhost:8080/topics
```

Создать topic:

```bash
curl -X POST http://localhost:8080/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders-http","partitions":1}'
```

## Как В Проекте Используются gRPC И HTTP

В проекте специально используются два разных сетевых слоя:

- `gRPC` — основной интерфейс для сценариев producer/consumer
- `HTTP` — простой интерфейс для проверки состояния и управления `topic`

Это сделано осознанно, потому что у этих интерфейсов разные задачи.

### Зачем Здесь gRPC

gRPC используется для основных операций брокера:

- `CreateTopic`
- `Produce`
- `Fetch`
- `CommitOffset`
- `GetCommittedOffset`

Почему именно gRPC:

- есть строгий контракт через protobuf
- удобно описывать интерфейс для producer/consumer клиентов
- проще контролировать структуру запросов и ответов
- это ближе по стилю к реальному взаимодействию сервисов, чем произвольный JSON-интерфейс

В проекте я работал с gRPC на нескольких уровнях:

- описал контракт в [api/proto/broker.proto](api/proto/broker.proto)
- сгенерировал Go-код клиента и сервера
- реализовал gRPC-обработчики в [internal/api/grpc/server.go](internal/api/grpc/server.go)
- сделал преобразование доменных ошибок в коды статуса gRPC в [internal/api/grpc/errors.go](internal/api/grpc/errors.go)
- добавил ограниченное чтение батчами через `max_records` и `next_offset`

То есть gRPC в проекте — это не просто обертка над функциями, а основной строго типизированный контракт брокера.

### Зачем Здесь HTTP

HTTP используется как легкий административный слой:

- `GET /health`
- `GET /topics`
- `POST /topics`

Почему HTTP тоже полезен:

- удобно быстро проверять брокер через `curl`
- удобно показывать проект на демо
- хорошо подходит для простых административных операций
- `health`-метод проще встраивать в обычные рабочие сценарии

HTTP часть реализована в:

- [internal/api/http/server.go](internal/api/http/server.go)
- [internal/api/http/handlers.go](internal/api/http/handlers.go)

### Как Эти Слои Между Собой Связаны

И gRPC, и HTTP не работают напрямую с файлами или storage.

Оба сетевых слоя вызывают один и тот же сервисный слой:

- [internal/broker/service.go](internal/broker/service.go)

Это важное архитектурное решение:

- сетевой слой отвечает за прием запроса и формирование ответа
- `broker.Service` отвечает за бизнес-логику
- слой хранения отвечает за чтение и запись данных

Благодаря этому:

- логика не дублируется между gRPC и HTTP
- сетевой слой остается тонким
- тестировать систему проще

### Путь Запроса Внутри Системы

Типичный путь запроса выглядит так:

```text
Client / CLI
    ->
gRPC-обработчик или HTTP-обработчик
    ->
broker.Service
    ->
metadata / group store / partition log
    ->
disk
```

Пример для `Produce`:

1. клиент отправляет gRPC `ProduceRequest`
2. gRPC handler валидирует и переводит запрос во внутренний вызов service layer
3. `broker.Service` проверяет `topic` и `partition`, затем получает нужный `PartitionLog`
4. слой хранения записывает запись в сегмент и индекс
5. handler возвращает клиенту `offset` и `timestamp`

Пример для admin create topic:

1. клиент отправляет HTTP `POST /topics`
2. HTTP handler декодирует JSON body
3. обработчик вызывает `broker.Service.CreateTopic(...)`
4. хранилище сведений создает запись о `topic` и директории `partition`
5. HTTP-слой возвращает JSON-ответ

## Тесты

Запуск всех тестов:

```bash
go test ./...
```

Или через `Makefile`:

```bash
make test
```

Сейчас тестами покрыты сценарии:

- очистка удаляет старые сегменты
- восстановление после удаления `.index`
- восстановление индекса для неактивного сегмента
- ограниченное чтение батчами
- основные сценарии сервисного слоя брокера
- основные сценарии и ограниченное чтение для gRPC
- основные сценарии для HTTP
- сохранение `offset` групп после повторного открытия

## Инженерные Решения

На текущем этапе в проекте уже есть несколько важных решений, которые делают его сильнее обычного учебного проекта:

- кэш открытых `PartitionLog` внутри `broker.Service`
- синхронизация доступа к общему изменяемому состоянию
- ограниченное чтение через `max_records`
- явный `next_offset` в `FetchResponse`
- валидация topic names до работы с файловой системой
- более безопасное сохранение метаданных и `offset` через `tmp + rename`
- восстановление индексов для всех сегментов при повторном открытии

## Ограничения 

Важно понимать текущие ограничения:

- брокер работает на одном узле
- `Produce` и `Fetch` требуют явный `partition`
- нет автоматического распределения по `partition`
- нет репликации и отказоустойчивости между узлами
- нет consumer rebalance
- нет `max_bytes` в чтении
- нет долгого ожидания или потокового чтения
- нет `GET /groups` и `GET /stats`
- надежность сохранения улучшена, но без полного усиления через `fsync`

## Команды Makefile

Если удобнее запускать через `make`:

```bash
make run-broker
make run-broker-config
make build
make fmt
make test
make coverage
make proto
```

## Связанные Документы

- [README_ARCHITECTURE.md](README_ARCHITECTURE.md) — подробная архитектура проекта
- [README_NETWORK.md](README_NETWORK.md) — сетевая часть и API
- [README_PLAN.md](README_PLAN.md) — roadmap
- [README_TASK.md](README_TASK.md) — текущий этап разработки
