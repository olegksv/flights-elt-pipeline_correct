# End-to-End Flights ELT Pipeline (Dagster + dbt + PostgreSQL)

Автоматизированный сквозной ELT-пайплайн для сбора, очистки и витринизации данных об авиаперевозках с обогащением актуальными курсами валют из внешнего API ЦБ РФ.

## Архитектура пайплайна

```text
[Postgres: bookings.flights] ──────┐
[Postgres: bookings.ticket_flights] ├──► [dbt: staging (views)] ──► [dbt: mart_route_revenue] ──► [Data Quality Tests]
[API ЦБ РФ: USD/RUB] ─► [Python] ───┘         (Очистка)               (Инкрементальная витрина)        (not_null)
```

### Основные этапы:
1. **Extract & Load (`Python` + `Requests` + `SQLAlchemy`):** Ассет `raw_currency_rates` забирает актуальный курс USD/RUB из JSON-фида ЦБ РФ с использованием сессии с повторными попытками (`Retry`) и загружает данные в сырой слой PostgreSQL.
2. **Transform (`dbt-postgres`):**
   * **Staging-слой (`view`):** очистка сырых данных о рейсах (`stg_flights`) и билетах (`stg_ticket_flights`), фильтрация отмененных рейсов и приведение типов.
   * **Marts-слой (`incremental`):** витрина `mart_route_revenue` рассчитывает количество проданных билетов и выручку по маршрутам в рублях и долларах США с поддержкой инкрементального обновления (`unique_key`).
3. **Data Quality & Orchestration (`Dagster`):** Единый граф зависимостей (Data Lineage), автоматический запуск `dbt` тестов качества данных (`not_null`) при каждой сборке и ежедневное обновление по `cron`-расписанию (`0 4 * * *`).

## Стек технологий
* **Оркестрация:** Dagster (`dg` Components, `dagster-dbt`)
* **Трансформации и тестирование:** dbt-postgres (Jinja SQL, Incremental models, Schema tests)
* **Извлечение данных:** Python 3.12, Pandas, Requests, SQLAlchemy (`psycopg2`)
* **Хранилище:** PostgreSQL (демо-база авиаперевозок `demo`)
* **Управление зависимостями:** `uv`

## Локальный запуск

1. **Клонируйте репозиторий и установите зависимости:**
   ```bash
   git clone <ваш-url-репозитория>
   cd flights-elt-pipeline
   uv sync
   ```

2. **Настройте переменные окружения:**
   Скопируйте шаблон `.env.example` в `.env` и укажите параметры подключения к вашей базе PostgreSQL:
   ```bash
   cp .env.example .env
   ```

3. **Запустите веб-интерфейс Dagster:**
   ```bash
   uv run --env-file .env dg dev
   ```
   После запуска откройте `http://localhost:3000`, перейдите во вкладку **Lineage** и нажмите **Materialize all**.
