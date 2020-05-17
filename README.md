README
======

Зависимости:
------------

- python3

- clickhouse_driver - для взаимодействия с ClickHouse

  ```sh
  # установить clickhouse_driver через pip
  sudo pip3 install clickhouse_driver
  ```

- curl - для тестирования

Использование:
--------------

1. Запустить `clickhouse-proxy`

2. Отправить запрос при помощи **CURL**:

   ```sh
   # с JSON-объектом из строки
   curl \
   -w '\n' \
   -X POST \
   -H 'Content-Type: application/json' \
   -d '{"query_id": "54413062-60bf-4c1e-9d19-34d6948647a4"}' \
   localhost:8000/api/status
   
   # с JSON-объектом из файла './data.json'
   curl \
   -w '\n' \
   -X POST \
   -H 'Content-Type: application/json' \
   -d '@data.json' \
   localhost:8000/api/status

   # с JSON-объектом из stdin
   cat | curl \
   -w '\n' \
   -X POST \
   -H 'Content-Type: application/json' \
   -d '@-' \
   localhost:8000/api/status
   ```

