# 📚 Учебно-методическое пособие: Лабораторная работа №5 (Версия 2.0)
## Тема: Инкрементальный ELT-пайплайн, версионирование SCD Type 2 и Data Quality Gates

- **Дисциплина:** Интеллектуальные хранилища данных / Data Engineering
- **Кафедра:** Прикладной информатики (НГУЭУ) | Лаборатория: nsuemdm-lab
- **Технологический стек:** PostgreSQL 14+, Python 3.10+, SQLAlchemy 2.0+, Pandas, Psycopg2, Ubuntu Linux / Colab

---
## 📌 Аннотация и контекст курса

Данное учебно-методическое пособие представляет собой глубокий технический и архитектурный разбор **Лабораторной работы №5** из практического цикла дисциплины **«Интеллектуальные хранилища данных / Data Engineering»** (Кафедра Data Management, НГУЭУ).

В рамках этой работы проектируется и реализуется сквозной внутрибазовый **ELT-конвейер (Extract-Load-Transform)** для обработки корпоративной финансовой отчетности, поддержания темпоральной истории измерений по стандарту **SCD Type 2**, обеспечения **идемпотентности** обработки и контроля качества данных (**Data Quality Gates**).

---
## 📑 Оглавление

1. [Теоретический базис и паттерны проектирования DWH](#1-теоретический-базис-и-паттерны-проектирования-dwh)
   - [1.1. Эволюция архитектур: ETL vs ELT в эпоху Big Data](#11-эволюция-архитектур-etl-vs-elt-в-эпоху-big-data)
   - [1.2. Версионирование измерений: Slowly Changing Dimensions (SCD Type 2)](#12-версионирование-измерений-slowly-changing-dimensions-scd-type-2)
   - [1.3. Принцип идемпотентности и защита от дубликатов (Zero-Duplication)](#13-принцип-идемпотентности-и-защита-от-дубликатов-zero-duplication)
   - [1.4. Слоистая архитектура DWH (Layered Architecture)](#14-слоистая-архитектура-dwh-layered-architecture)
2. [Архитектурный контракт и DDL-схема Хранилища](#2-архитектурный-контракт-и-ddl-схема-хранилища)
3. [Пошаговый разбор реализации и программного кода](#3-пошаговый-разбор-реализации-и-программного-кода)
   - [Этап 0: Инициализация окружения и пула соединений SQLAlchemy](#этап-0-инициализация-окружения-и-пула-соединений-sqlalchemy)
   - [Этап 1: Физическое развертывание схем и темпоральных таблиц (DDL Migration)](#этап-1-физическое-развертывание-схем-и-темпоральных-таблиц-ddl-migration)
   - [Этап 2: Транзакционная ротация версий измерений (SCD Type 2 Logic)](#этап-2-транзакционная-ротация-версий-измерений-scd-type-2-logic)
   - [Этап 3: Высокоскоростной пакетный Extract & Load в Staging](#этап-3-высокоскоростной-пакетный-extract--load-в-staging)
   - [Этап 4: Внутрибазовый ELT-перелив в Core (Single Transaction Transform)](#этап-4-внутрибазовый-elt-перелив-в-core-single-transaction-transform)
   - [Этап 5: Построение семантической аналитической витрины (Data Mart)](#этап-5-построение-семантической-аналитической-витрины-data-mart)
   - [Этап 6: Автоматизированный аудит качества (DQ Gates) и тест идемпотентности](#этап-6-автоматизированный-аудит-качества-dq-gates-и-тест-идемпотентности)
   - [Этап 7: Фиксация эталонного состояния (Disaster Recovery Snapshot)](#этап-7-фиксация-эталонного-состояния-disaster-recovery-snapshot)
4. [Практика Big Data: Масштабирование на промышленные объемы](#4-практика-big-data-масштабирование-на-промышленные-объемы)
5. [Чек-лист готовности и Definition of Done (DoD)](#5-чек-лист-готовности-и-definition-of-done-dod)

---
## 1. Теоретический базис и паттерны проектирования DWH

### 1.1. Эволюция архитектур: ETL vs ELT в эпоху Big Data

В традиционных архитектурах аналитических систем долгое время доминировал подход **ETL (Extract, Transform, Load)**:
1. **Extract:** Извлечение данных из OLTP-источников.
2. **Transform:** Манипуляции, фильтрация, очистка и объединение данных на промежуточном сервере интеграции (Informatica, SSIS, Talend).
3. **Load:** Загрузка уже трансформированных данных в DWH.

#### Проблема ETL в Big Data:
При работе с высокими объемами данных (терабайты/петабайты) промежуточный ETL-сервер становится **бутылочным горлышком (bottleneck)**. Сетевая сериализация, десериализация и передача огромных массивов данных между хранилищем и промежуточным сервером приводят к катастрофическому падению производительности.

```
Source ──(Extract)──> ETL Intermediate Server ──(Transform)──> DWH Storage (Load)
```

#### Преимущество ELT (Extract, Load, Transform):
В подходе **ELT** сырые данные сначала напрямую загружаются в буферный слой (**Staging Area**) внутри СУБД/MPP-системы. Вся тяжелая трансформация, агрегация и связывание происходят **внутри базы данных (In-Database Transformation)** с использованием мощностей SQL-движка аналитического хранилища (PostgreSQL, Greenplum, ClickHouse, Snowflake, BigQuery). Это исключает лишние сетевые перегонки данных и позволяет параллелить SQL-операции.

```
Source ──(Extract & Load)──> DWH Staging Area ──(In-Database SQL Transform)──> DWH Core / Marts
```

---
### 1.2. Версионирование измерений: Slowly Changing Dimensions (SCD Type 2)

В корпоративных хранилищах данных бизнес-сущности (компании, клиенты, товары) с течением времени меняют свои атрибуты.
*Пример:* Компания `YNDX` сменила юридическое наименование с `Yandex N.V.` на `МКПАО Яндекс` и сектор с `Information Technology` на `Communication Services`.

Если просто перезаписать атрибут (**SCD Type 1**), историческая аналитика исказится: финансовые отчеты за 2020–2023 годы ошибочно будут приписаны новому юридическому лицу или новому сектору.

Для сохранения полной исторической правды используется **SCD Type 2 (Медленно меняющиеся измерения 2-го типа)**:

| company_sk (PK) | ticker (Natural Key) | company_name | gics_sector | valid_from | valid_to | is_current |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 101 | YNDX | Yandex N.V. | Information Technology | 2020-01-01 | 2023-12-31 | FALSE |
| 102 | YNDX | МКПАО Яндекс | Communication Services | 2024-01-01 | NULL | TRUE |

#### Ключевые компоненты SCD Type 2:
1. **Суррогатный ключ (`company_sk`):** Уникальный целочисленный идентификатор конкретной *версии* записи.
2. **Интервал актуальности (`valid_from`, `valid_to`):** Границы времени, в рамках которых версия являлась истинной.
3. **Флаг активности (`is_current`):** Булевый маркер текущей рабочей версии (`TRUE` — активная, `FALSE` — закрытая historical).

#### Гарантия целостности в СУБД:
Чтобы исключить появление нескольких одновременно «активных» версий для одного тикера при параллельной записи, применяется **частичный уникальный индекс (Partial Unique Index)**:

```sql
CREATE UNIQUE INDEX uq_active_company_ticker ON core.dim_company(ticker) WHERE is_current = TRUE;
```

---
### 1.3. Принцип идемпотентности и защита от дубликатов (Zero-Duplication)

**Идемпотентность** — это свойство операции давать один и тот же результат при многократном повторном вызове с одними и теми же входными данными:

$$f(f(x)) = f(x)$$

В Data Engineering сбои сети, падения тасок в оркестраторах (Apache Airflow) или сбои в БД — регулярные события. Если конвейер упал на середине, он должен уметь перезапуститься без дублирования данных.

#### Техники обеспечения идемпотентности:
1. **DML Upsert (`ON CONFLICT`):** Обновление или пропуск существующих ключей при синхронизации измерений.
2. **Subquery Filtering (`WHERE NOT EXISTS`):** Вставка фактов только в том случае, если сочетание `(company_sk, metric_id, report_date)` еще не существует.
3. **Атомарность транзакций (`BEGIN...COMMIT / ROLLBACK`):** Выполнение всех шагов трансформации в едином транзакционном блоке.

---
### 1.4. Слоистая архитектура DWH (Layered Architecture)

Хранилище данных проектируется с четким разделением ответственности по слоям:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA (CSV / APIs)                        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Extract & Batch Load (Pandas / to_sql)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGING LAYER (staging.raw_financial_feed)                              │
│ • Loose VARCHAR typing, no Foreign Keys, ephemeral buffer              │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ In-Database ELT SQL Transformation
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ CORE LAYER (Normalized Enterprise Warehouse)                           │
│ • core.dim_company (SCD Type 2)                                        │
│ • core.dim_metric_dictionary (Normalized Dictionary)                   │
│ • core.fact_financial_metrics (EAV Fact Table, FK references)          │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Semantic Aggregation & Unpivoting
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ MARTS / SANDBOX LAYER (sandbox.view_corporate_performance)             │
│ • Unpivoted Analytical Metrics, Calculated Ratios (EBITDA Margin %)    │
└────────────────────────────────────────────────────────────────────────┘
```

1. **Staging (`staging`):** Входной буфер для оперативного приема пакетов (`VARCHAR`, без FK).
2. **Core (`core`):** Нормализованное ядро хранилища (SCD-2 измерение компаний, справочник метрик, EAV-факты).
3. **Sandbox / Marts (`sandbox`):** Слой витрин данных с развернутыми EAV-структурами в столбцы.

---
## 2. Архитектурный контракт и DDL-схема Хранилища

Архитектурный контракт DWH регламентирует форматы данных, ограничения целостности и принципы типизации на каждом слое.

### 2.1. Контракты типизации: Свободная vs Строгая типизация
- **Слой Staging (`staging.raw_financial_feed`):** Применяет паттерн **Schema-on-Read / Delayed Validation**. Все поля объявляются как `VARCHAR`. Это гарантирует, что даже если источник передаст дату в нестандартном формате, некорректную строку или численный форматированный текст (например, `"100 000,50"`), этап первичной инжекции не рухнет с фатальной ошибкой `Invalid Text Representation`. Данные гарантированно попадают в буфер, откуда их можно валидировать, трансформировать или отсеять в карантин.
- **Слой Core (`core`):** Применяет паттерн **Schema-on-Write / Strict Typing**. Поля приводятся к точным типам (`DATE`, `NUMERIC(18,4)`, `INTEGER`). Использование `NUMERIC(18,4)` вместо `FLOAT/DOUBLE PRECISION` является стандартом для финансовых хранилищ, так как предотвращает накапливание ошибок округления двоичной плавающей запятой.

### 2.2. Полный DDL-скрипт создания структуры DWH

```sql
-- Создание изолированных схем хранилища
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS sandbox;

-- 1. Буферная таблица приема сырых данных (Staging Layer)
DROP TABLE IF EXISTS staging.raw_financial_feed CASCADE;
CREATE TABLE staging.raw_financial_feed (
    feed_id         BIGSERIAL PRIMARY KEY,
    ticker          VARCHAR(50),
    company_name    VARCHAR(255),
    gics_sector     VARCHAR(100),
    report_date     VARCHAR(50),
    metric_code     VARCHAR(100),
    metric_value    VARCHAR(100),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Измерение компаний с поддержкой SCD Type 2 (Core Layer)
DROP TABLE IF EXISTS core.dim_company CASCADE;
CREATE TABLE core.dim_company (
    company_sk      SERIAL PRIMARY KEY,          -- Суррогатный ключ версии
    ticker          VARCHAR(10) NOT NULL,        -- Естественный бизнес-ключ (Natural Key)
    company_name    VARCHAR(255) NOT NULL,
    gics_sector     VARCHAR(100) NOT NULL,
    valid_from      DATE NOT NULL,               -- Начало интервала актуальности
    valid_to        DATE,                        -- Конец интервала актуальности (NULL = активна)
    is_current      BOOLEAN DEFAULT TRUE         -- Флаг текущей активной версии
);

-- Частичный уникальный индекс: гарантирует ровно одну активную версию для каждого тикера
CREATE UNIQUE INDEX uq_active_company_ticker 
ON core.dim_company(ticker) 
WHERE is_current = TRUE;

-- 3. Нормализованный справочник финансовых показателей (Core Layer)
DROP TABLE IF EXISTS core.dim_metric_dictionary CASCADE;
CREATE TABLE core.dim_metric_dictionary (
    metric_id       SERIAL PRIMARY KEY,
    metric_code     VARCHAR(50) UNIQUE NOT NULL,
    metric_name     VARCHAR(255) NOT NULL,
    unit            VARCHAR(20) DEFAULT 'RUB_MLN'
);

-- 4. Таблица фактов финансовых показателей в EAV-формате (Core Layer)
DROP TABLE IF EXISTS core.fact_financial_metrics CASCADE;
CREATE TABLE core.fact_financial_metrics (
    fact_id         BIGSERIAL PRIMARY KEY,
    company_sk      INT NOT NULL REFERENCES core.dim_company(company_sk),
    metric_id       INT NOT NULL REFERENCES core.dim_metric_dictionary(metric_id),
    report_date     DATE NOT NULL,
    metric_value    NUMERIC(18,4) NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Составное ограничение уникальности для предотвращения дубликатов (Zero-Duplication Constraint)
    CONSTRAINT uq_fact_record UNIQUE (company_sk, metric_id, report_date)
);
```

---
## 3. Пошаговый разбор реализации и программного кода

### Этап 0: Инициализация окружения и пула соединений SQLAlchemy

#### Практическая реализация:
В первом этапе подготавливается Linux-окружение Colab, проверяется и запускается локальный демон PostgreSQL, создает учетная запись администратора `dwh_admin` и БД `elt_dwh`. Затем инициализируется пул соединений Python при помощи SQLAlchemy 2.0.

```python
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Автоматический запуск службы PostgreSQL в Colab / Ubuntu
subprocess.run(["service", "postgresql", "start"], check=True)

# 2. Создание пользователя и базы данных через psql
setup_psql_cmd = """
DO $$ 
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dwh_admin') THEN
      CREATE ROLE dwh_admin WITH LOGIN PASSWORD 'admin123' SUPERUSER;
   END IF;
END $$;
"""
subprocess.run(f'sudo -u postgres psql -c "{setup_psql_cmd}"', shell=True, check=True)
subprocess.run('sudo -u postgres psql -c "CREATE DATABASE elt_dwh OWNER dwh_admin;"', shell=True, check=False)

# 3. Настройка SQLAlchemy Engine с пулом соединений
DB_URL = "postgresql+psycopg2://dwh_admin:admin123@localhost:5432/elt_dwh"

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,  # Автоматическая проверка валидности соединения перед вызовом (Prevent Stale Connections)
    pool_size=5,         # Базовый размер пула подключений
    max_overflow=10      # Максимальный лимит дополнительных временных соединений при пиковой нагрузке
)

# Проверка физического отклика БД
with engine.begin() as conn:
    res = conn.execute(text("SELECT version();")).scalar()
    print(f"PostgreSQL успешно подключен:\n{res}")
```

#### Архитектурно-теоретический разбор:
- **Пул соединений (Connection Pooling):** В Big Data и высоконагруженных бэкендах установка нового TCP-соединения к PostgreSQL с выполнением процедур рукопожатия (handshake) и аутентификации — это ресурсоемкая операция. SQLAlchemy Engine поддерживает пул готовых открытых соединений (`pool_size`), что снижает задержку (latency) при частых запросах.
- **`pool_pre_ping=True`:** Обеспечивает устойчивость к обрывам сети и переподключениям. Перед передачей соединения из пула клиенту отправляется легкий `ping` (`SELECT 1`). Если сетевое соединение разведено брандмауэром или перезапуском БД, оно прозрачно пересоздается.
- **Транзакционный контекст `with engine.begin() as conn:`:** Использование контекстного менеджера SQLAlchemy гарантирует явное открытие транзакции `BEGIN` и её автоматический `COMMIT` при успешном выходе из блока или `ROLLBACK` при возникновении любого необработанного исключения.

---

### Этап 1: Физическое развертывание схем и темпоральных таблиц (DDL Migration)

#### Практическая реализация:
Выполняется генерация и накатывание DDL-миграции в хранилище.

```python
ddl_migration_sql = """
BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS sandbox;

CREATE TABLE IF NOT EXISTS staging.raw_financial_feed (
    feed_id         BIGSERIAL PRIMARY KEY,
    ticker          VARCHAR(50),
    company_name    VARCHAR(255),
    gics_sector     VARCHAR(100),
    report_date     VARCHAR(50),
    metric_code     VARCHAR(100),
    metric_value    VARCHAR(100),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.dim_company (
    company_sk      SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL,
    company_name    VARCHAR(255) NOT NULL,
    gics_sector     VARCHAR(100) NOT NULL,
    valid_from      DATE NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_company_ticker 
ON core.dim_company(ticker) 
WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS core.dim_metric_dictionary (
    metric_id       SERIAL PRIMARY KEY,
    metric_code     VARCHAR(50) UNIQUE NOT NULL,
    metric_name     VARCHAR(255) NOT NULL,
    unit            VARCHAR(20) DEFAULT 'RUB_MLN'
);

CREATE TABLE IF NOT EXISTS core.fact_financial_metrics (
    fact_id         BIGSERIAL PRIMARY KEY,
    company_sk      INT NOT NULL REFERENCES core.dim_company(company_sk),
    metric_id       INT NOT NULL REFERENCES core.dim_metric_dictionary(metric_id),
    report_date     DATE NOT NULL,
    metric_value    NUMERIC(18,4) NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_record UNIQUE (company_sk, metric_id, report_date)
);

COMMIT;
"""

with engine.begin() as conn:
    conn.execute(text(ddl_migration_sql))
    print("[DDL MIGRATION] Схемы и таблицы успешно созданы.")
```

#### Архитектурно-теоретический разбор:
- **Почему Частичный Уникальный Индекс лучше триггера?** В традиционных БД часто пытаются контролировать уникальность активной версии через PL/pgSQL триггеры `BEFORE INSERT OR UPDATE`. Однако при высокой интенсивности вставки триггеры вызывают построчную блокировку таблиц (Row-Level Locking) и снижают throughput. Частичный уникальный индекс `WHERE is_current = TRUE` проверяет целостность на уровне B-Tree индекса СУБД с минимальным overhead и полной поддержкой MVCC (Multi-Version Concurrency Control).

---

### Этап 2: Транзакционная ротация версий измерений (SCD Type 2 Logic)

#### Практическая реализация:
Создается Python-функция `scd2_update_company`, реализующая бизнес-логику закрытия устаревшей версии измерения и отрытия нового темпорального интервала.

```python
def scd2_update_company(engine, ticker: str, new_name: str, new_sector: str, effective_date: str):
    """
    Реализует транзакционную ротацию версий SCD Type 2.
    Если атрибуты изменились — закрывает текущую версию и создает новую.
    """
    check_sql = text("""
        SELECT company_sk, company_name, gics_sector 
        FROM core.dim_company 
        WHERE ticker = :ticker AND is_current = TRUE;
    """)
    
    close_old_version_sql = text("""
        UPDATE core.dim_company
        SET valid_to = :effective_date::date - INTERVAL '1 day',
            is_current = FALSE
        WHERE ticker = :ticker AND is_current = TRUE;
    """)
    
    insert_new_version_sql = text("""
        INSERT INTO core.dim_company (ticker, company_name, gics_sector, valid_from, valid_to, is_current)
        VALUES (:ticker, :new_name, :new_sector, :effective_date::date, NULL, TRUE);
    """)

    with engine.begin() as conn:
        active_rec = conn.execute(check_sql, {"ticker": ticker}).fetchone()
        
        if active_rec:
            curr_sk, curr_name, curr_sector = active_rec
            # Если атрибуты не изменились — ротация не требуется
            if curr_name == new_name and curr_sector == new_sector:
                print(f"[SCD2] Для тикера {ticker} изменений не обнаружено. Пропуск.")
                return
            
            # Атрибуты изменились — выполняем транзакционную ротацию
            conn.execute(close_old_version_sql, {"ticker": ticker, "effective_date": effective_date})
            conn.execute(insert_new_version_sql, {
                "ticker": ticker, "new_name": new_name, "new_sector": new_sector, "effective_date": effective_date
            })
            print(f"[SCD2 ROTATION] Успешно обновлена версия для {ticker}. Дата вступления: {effective_date}")
        else:
            # Первая первичная вставка компании
            conn.execute(insert_new_version_sql, {
                "ticker": ticker, "new_name": new_name, "new_sector": new_sector, "effective_date": effective_date
            })
            print(f"[SCD2 INITIAL] Первичная регистрация компании {ticker}.")
```

#### Архитектурно-теоретический разбор:
- **Атомарность ротации:** Изменение поля `valid_to` у старой записи и `INSERT` новой записи выполняются в **одной транзакции**. Если в момент вставки произойдет сбой (например, отключение питания), транзакция откатится, и хранилище не останется в противоречивом состоянии с «закрытой старой записью без новой».
- **Time-Travel Кверирование:** Благодаря сохранению истории с метками времени, аналитик может выполнить запрос состояния измерений на любую историческую дату:
  ```sql
  SELECT * FROM core.dim_company 
  WHERE '2023-06-15' BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31');
  ```

---

### Этап 3: Высокоскоростной пакетный Extract & Load в Staging

#### Практическая реализация:
Подготавливается входящий сырой пакет данных в формате CSV (симуляция внешней системы), который загружается в `staging.raw_financial_feed`.

```python
import io

# Симуляция сырого дельта-пакета (T0) от финансового провайдера
raw_csv_payload = """ticker,company_name,gics_sector,report_date,metric_code,metric_value
YNDX,Yandex N.V.,Information Technology,2023-12-31,REVENUE,300000.50
YNDX,Yandex N.V.,Information Technology,2023-12-31,EBITDA,85000.00
YNDX,Yandex N.V.,Information Technology,2023-12-31,NET_PROFIT,45000.00
SBER,Сбербанк,Financials,2023-12-31,REVENUE,1500000.00
SBER,Сбербанк,Financials,2023-12-31,NET_PROFIT,1200000.00
GAZP,Газпром,Energy,2023-12-31,REVENUE,8000000.00
GAZP,Газпром,Energy,2023-12-31,EBITDA,1200000.00
"""

def load_staging_batch(engine, csv_data: str):
    """
    Высокоскоростная загрузка в Staging без использования медленного iterrows().
    """
    df_incoming = pd.read_csv(io.StringIO(csv_data))
    
    # Использование векторной пакетной вставки to_sql
    df_incoming.to_sql(
        name='raw_financial_feed',
        schema='staging',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000  # Отправка пакетами по 1000 строк в одном SQL INSERT
    )
    print(f"[STAGING LOAD] Успешно загружено {len(df_incoming)} записей в staging.raw_financial_feed.")

load_staging_batch(engine, raw_csv_payload)
```

#### Архитектурно-теоретический разбор:
- **Антипатерн `df.iterrows()`:** Вызов `for index, row in df.iterrows(): conn.execute(...)` производит отдельный сетевой вызов к СУБД для каждой строки. Загрузка 1 млн строк таким методом занимает десятки минут.
- **Пакетный метод `method='multi'` с `chunksize=1000`:** Группирует 1000 строк в единый SQL-запрос `INSERT INTO ... VALUES (...), (...), (...)`. Это сокращает сетевой оверхед в сотни раз и полностью утилизирует пропускную способность соединения.

---

### Этап 4: Внутрибазовый ELT-перелив в Core (Single Transaction Transform)

#### Практическая реализация:
Выполняется атомарная внутрибазовая трансформация из `staging` в `core`.

```python
elt_transform_sql = """
BEGIN;

-- 1. Синхронизация нормализованного справочника метрик (DML Upsert)
INSERT INTO core.dim_metric_dictionary (metric_code, metric_name)
SELECT DISTINCT 
    metric_code,
    CASE 
        WHEN metric_code = 'REVENUE' THEN 'Выручка компании'
        WHEN metric_code = 'EBITDA' THEN 'Операционная прибыль до вычета (EBITDA)'
        WHEN metric_code = 'NET_PROFIT' THEN 'Чистая прибыль'
        ELSE metric_code
    END AS metric_name
FROM staging.raw_financial_feed
ON CONFLICT (metric_code) DO NOTHING;

-- 2. Первичная инжекция новых компаний в dim_company (если тикера еще нет в хранилище)
INSERT INTO core.dim_company (ticker, company_name, gics_sector, valid_from, is_current)
SELECT DISTINCT 
    s.ticker,
    s.company_name,
    s.gics_sector,
    s.report_date::date AS valid_from,
    TRUE AS is_current
FROM staging.raw_financial_feed s
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_company c WHERE c.ticker = s.ticker
);

-- 3. Перелив фактов с динамическим связыванием по интервалам SCD-2 и защитой Zero-Duplication
INSERT INTO core.fact_financial_metrics (company_sk, metric_id, report_date, metric_value)
SELECT 
    c.company_sk,
    m.metric_id,
    s.report_date::date,
    s.metric_value::numeric(18,4)
FROM staging.raw_financial_feed s
-- Динамический Join с версией измерения, актуальной на дату финансового отчета
JOIN core.dim_company c 
    ON s.ticker = c.ticker
   AND s.report_date::date >= c.valid_from 
   AND (c.valid_to IS NULL OR s.report_date::date <= c.valid_to)
JOIN core.dim_metric_dictionary m 
    ON s.metric_code = m.metric_code
-- Фильтр идемпотентности: исключение уже загруженных фактов
WHERE NOT EXISTS (
    SELECT 1 
    FROM core.fact_financial_metrics f
    WHERE f.company_sk = c.company_sk
      AND f.metric_id = m.metric_id
      AND f.report_date = s.report_date::date
);

-- 4. Автоматическая гигиена буферного слоя
TRUNCATE TABLE staging.raw_financial_feed;

COMMIT;
"""

with engine.begin() as conn:
    conn.execute(text(elt_transform_sql))
    print("[ELT TRANSFORM] Перелив из Staging в Core выполнен атомарно. Staging очищен.")
```

#### Архитектурно-теоретический разбор:
- **Динамический Temporal Join:** Соединение фактов с измерением выполняется не просто по естественному ключу `s.ticker = c.ticker`, а с учетом временного окна актуальности версии `report_date BETWEEN valid_from AND valid_to`. Это гарантирует, что факт 2021 года привяжется к версии компании, существовавшей в 2021 году, а факт 2024 года — к актуальной версии 2024 года.
- **Внутрибазовая трансформация (In-Database Computation):** Вся работа по приведению типов, фильтрации и объединению выполняется высокооптимизированным движком PostgreSQL в оперативной памяти сервера БД без выгрузки данных на клиентский Python-сервер.

---

### Этап 5: Построение семантической аналитической витрины (Data Mart)

#### Практическая реализация:
Создается аналитическая витрина `sandbox.view_corporate_performance`, транспонирующая вертикальную EAV-структуру в широкую таблицы и рассчитывающая KPI.

```python
mart_sql = """
CREATE OR REPLACE VIEW sandbox.view_corporate_performance AS
WITH base_pivot AS (
    SELECT 
        c.ticker,
        c.company_name,
        c.gics_sector,
        f.report_date,
        MAX(CASE WHEN m.metric_code = 'REVENUE' THEN f.metric_value END) AS revenue,
        MAX(CASE WHEN m.metric_code = 'EBITDA' THEN f.metric_value END) AS ebitda,
        MAX(CASE WHEN m.metric_code = 'NET_PROFIT' THEN f.metric_value END) AS net_profit
    FROM core.fact_financial_metrics f
    JOIN core.dim_company c ON f.company_sk = c.company_sk
    JOIN core.dim_metric_dictionary m ON f.metric_id = m.metric_id
    GROUP BY c.ticker, c.company_name, c.gics_sector, f.report_date
)
SELECT 
    ticker,
    company_name,
    gics_sector,
    report_date,
    revenue,
    ebitda,
    net_profit,
    -- Расчет аналитического показателя маржинальности EBITDA Margin %
    ROUND((ebitda / NULLIF(revenue, 0)) * 100, 2) AS ebitda_margin_pct
FROM base_pivot;
"""

with engine.begin() as conn:
    conn.execute(text(mart_sql))
    print("[DATA MART] Аналитическая витрина sandbox.view_corporate_performance создана.")
    
# Проверка выгрузки из витрины в Pandas
df_mart = pd.read_sql("SELECT * FROM sandbox.view_corporate_performance;", engine)
print(df_mart.to_string())
```

#### Архитектурно-теоретический разбор:
- **Транспонирование EAV (Conditional Aggregation / Pivot):** Структура Entity-Attribute-Value удобна для хранения фактов с динамическим набором атрибутов, но неудобна для BI-систем (Tableau, PowerBI, Superset). Использование `MAX(...) FILTER (WHERE ...)` или `MAX(CASE WHEN ...)` преобразует EAV-строки в традиционные колонки.
- **Защита от деления на ноль (`NULLIF`):** Конструкция `NULLIF(revenue, 0)` возвращает `NULL`, если выручка равна 0. Это предотвращает возникновение фатальной ошибки `Division by Zero` в SQL-запросах.

---

### Этап 6: Автоматизированный аудит качества (DQ Gates) и тест идемпотентности

#### Практическая реализация:
Выполняются проверки правил Data Quality и тест устойчивости конвейера к повторному прогону данных.

```python
dq_assertions_sql = """
SELECT 'Orphan Facts' AS check_name, COUNT(*) AS violations
FROM core.fact_financial_metrics f
LEFT JOIN core.dim_company c ON f.company_sk = c.company_sk
WHERE c.company_sk IS NULL

UNION ALL

SELECT 'Negative Revenue', COUNT(*)
FROM core.fact_financial_metrics f
JOIN core.dim_metric_dictionary m ON f.metric_id = m.metric_id
WHERE m.metric_code = 'REVENUE' AND f.metric_value < 0

UNION ALL

SELECT 'Net Profit > Revenue', COUNT(*)
FROM sandbox.view_corporate_performance
WHERE net_profit > revenue;
"""

def run_dq_gates(engine):
    df_dq = pd.read_sql(text(dq_assertions_sql), engine)
    print("\n=== DATA QUALITY GATES REPORT ===")
    print(df_dq.to_string(index=False))
    
    total_violations = df_dq['violations'].sum()
    if total_violations == 0:
        print("✅ [DQ PASSED] Все проверочные контракты выполнены успешно.")
    else:
        print(f"❌ [DQ FAILED] Обнаружено {total_violations} нарушений качества данных!")

# 1. Запуск первичной проверки DQ
run_dq_gates(engine)

# 2. Тестирование идемпотентности (Zero-Duplication Test)
print("\n[ИДЕМПОТЕНТНОСТЬ] Запуск повторной инжекции того же дельта-пакета...")
count_before = pd.read_sql("SELECT COUNT(*) FROM core.fact_financial_metrics;", engine).iloc[0, 0]

load_staging_batch(engine, raw_csv_payload)
with engine.begin() as conn:
    conn.execute(text(elt_transform_sql))

count_after = pd.read_sql("SELECT COUNT(*) FROM core.fact_financial_metrics;", engine).iloc[0, 0]

print(f"Количество фактов ДО: {count_before} | ПОСЛЕ: {count_after}")
assert count_before == count_after, "ОШИБКА: Идемпотентность нарушена, возникли дубликаты!"
print("✅ [IDEMPOTENCY PASSED] Повторный прогон не привел к дублированию данных.")
```

#### Архитектурно-теоретический разбор:
- **Circuit Breakers в Data Engineering:** DQ Gates выступают в роли «предохранителей». В реальном пайплайне при обнаружении нарушений (например, отрицательной выручки) конвейер автоматически прерывает свою работу и отправляет алерты в Telegram/Slack, защищая аналитические отчеты от загрязнения.

---

### Этап 7: Фиксация эталонного состояния (Disaster Recovery Snapshot)

#### Практическая реализация:
Выполняется экспорт схемы и данных хранилища в файл резервной копии.

```python
BACKUP_PATH = "/content/elt_dwh_backup.sql"

def create_dwh_backup():
    try:
        cmd = f"PGPASSWORD='admin123' pg_dump -h localhost -U dwh_admin -c elt_dwh > {BACKUP_PATH}"
        subprocess.run(cmd, shell=True, check=True)
        print(f"✅ [DISASTER RECOVERY] Дамп базы данных успешно сформирован: {BACKUP_PATH}")
    except Exception as e:
        print(f"❌ Ошибка создания дампа: {e}")

create_dwh_backup()
```

#### Архитектурно-теоретический разбор:
- **Логические vs Физические бекапы:** Программа `pg_dump` формирует логический дамп в виде набора SQL-инструкций `CREATE TABLE`, `INSERT`. Логические дампы идеальны для переносимости между версиями СУБД. В промышленных Big Data хранилищах логические дампы сочетают с физическим копированием файлов данных (WAL-архивирование) для реализации точки восстановления **PITR (Point-in-Time Recovery)**.

---
## 4. Практика Big Data: Масштабирование на промышленные объемы

При переносе описанных паттернов из PostgreSQL в масштабируемые распределенные СУБД (Greenplum, ClickHouse, Snowflake, Databricks Delta Lake) инженер Data Engineering должен учитывать следующие оптимизации:

### 1. Замена `SERIAL` суррогатных ключей на Hashing (MD5 / SHA-256)
В MPP-системах (Massively Parallel Processing) автоинкремент `SERIAL/BIGSERIAL` создает единую точку синхронизации между узлами кластера, что снижает производительность в разы. 
*Решение:* Формировать суррогатный ключ как хэш от естественного ключа и даты версии:
```sql
MD5(ticker || '|' || valid_from::text) AS company_sk
```

### 2. Партиционирование таблиц фактов (Table Partitioning)
Для ускорения аналитических запросов таблицы фактов секционируются по интервалам времени:
```sql
PARTITION BY RANGE (report_date)
```

### 3. Интеграция с метабазой и оркестраторами
В продуктивном контуре процессы загрузки из Staging в Core оборачиваются в задачи **Apache Airflow** (на базе `PostgresOperator` / `TaskGroup`) или компилируются с помощью фреймворка **dbt (data build tool)**.

---
## 5. Чек-лист готовности и Definition of Done (DoD)

Для подтверждения успешного выполнения Лабораторной работы №5 специалист должен проверить выполнение следующих пунктов:

- [x] Развернута трехслойная архитектура хранилища (`staging`, `core`, `sandbox`).
- [x] Таблица `core.dim_company` поддерживает версионирование по стандарту **SCD Type 2**.
- [x] Создан частичный уникальный индекс `uq_active_company_ticker` для гарантии единственной активной версии.
- [x] Реализована и протестирована процедура ротации версий `scd2_update_company`.
- [x] Инжекция в Staging осуществляется пакетным методом `to_sql` без `.iterrows()`.
- [x] ELT-перелив в `core` выполнен атомарно в рамках единой SQL-транзакции.
- [x] В витрине `sandbox.view_corporate_performance` реализована рассчитанная маржинальность EBITDA %.
- [x] Автоматизированы DQ Gates и подтверждена идемпотентность конвейера (Zero-Duplication).
- [x] Сформирован файл резервной копии `/content/elt_dwh_backup.sql`.

---
© **Кафедра Data Management (НГУЭУ)** | Учебно-методический комплект по курсу *Data Engineering & DWH*
