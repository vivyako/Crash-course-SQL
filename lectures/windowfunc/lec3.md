# Лекция: Фреймы в оконных функциях PostgreSQL

## Теоретическая часть

### Что такое фрейм (рамка окна)?

Фрейм (frame) — это подмножество строк внутри окна, которое определяет, какие именно строки будут участвовать в вычислении оконной функции для текущей строки. Если обычное окно (`PARTITION BY`) задает группу строк, то фрейм уточняет, какие строки из этой группы будут использованы.

**Ключевая идея:** Фрейм позволяет выполнять скользящие вычисления (moving calculations), где результат зависит от позиции текущей строки относительно других строк в окне.

### Когда фрейм используется?

- **Агрегатные оконные функции** (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`) — фрейм определяет набор строк для агрегации
- **Функции положения** (`FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`) — фрейм определяет границы поиска
- **Функции смещения** (`LAG`, `LEAD`) — фрейм **не влияет** на них (они всегда работают со всем окном)

### Синтаксис фрейма

```sql
function_name OVER (
    [PARTITION BY ...]
    [ORDER BY ...]
    [frame_clause]
)

frame_clause ::= 
    { ROWS | RANGE | GROUPS } frame_start
    { ROWS | RANGE | GROUPS } BETWEEN frame_start AND frame_end

frame_start ::= 
    UNBOUNDED PRECEDING
    | offset PRECEDING
    | CURRENT ROW

frame_end ::= 
    UNBOUNDED FOLLOWING
    | offset FOLLOWING
    | CURRENT ROW
```

### Типы фреймов

| Тип | Описание | Когда использовать |
|-----|----------|---------------------|
| `ROWS` | Физические строки (точное количество строк) | Когда важна точная позиция строки |
| `RANGE` | Логические диапазоны (по значению ORDER BY) | Когда важна логическая близость значений |
| `GROUPS` | Группы равных значений | Когда нужно работать с группами, а не отдельными строками |

### Параметры границ

| Граница | Описание |
|---------|----------|
| `UNBOUNDED PRECEDING` | Все строки от начала окна |
| `UNBOUNDED FOLLOWING` | Все строки до конца окна |
| `CURRENT ROW` | Текущая строка |
| `offset PRECEDING` | N строк/значений/групп перед текущей |
| `offset FOLLOWING` | N строк/значений/групп после текущей |

### Важные особенности

1. **ORDER BY обязателен**: для использования фрейма почти всегда требуется `ORDER BY`
2. **Фрейм по умолчанию**: если указан `ORDER BY` но не указан фрейм, PostgreSQL использует:
   - Для агрегатных функций: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
   - Для функций положения: зависит от конкретной функции
3. **NULL значения**: при использовании `RANGE` с `ORDER BY` NULL считаются равными и попадают в одну группу

---

## Практические примеры

### Подготовка данных

```sql
-- Создаем таблицу с продажами
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    sale_date DATE,
    amount NUMERIC(10,2)
);

-- Заполняем тестовыми данными
INSERT INTO sales (product_name, sale_date, amount) VALUES
    ('Ноутбук', '2024-01-01', 100),
    ('Ноутбук', '2024-01-02', 150),
    ('Ноутбук', '2024-01-03', 120),
    ('Ноутбук', '2024-01-04', 180),
    ('Ноутбук', '2024-01-05', 200),
    ('Ноутбук', '2024-01-06', 170),
    ('Мышь', '2024-01-01', 30),
    ('Мышь', '2024-01-02', 45),
    ('Мышь', '2024-01-03', 50),
    ('Мышь', '2024-01-04', 40),
    ('Мышь', '2024-01-05', 55),
    ('Мышь', '2024-01-06', 60);
```

---

## 1. Фреймы на основе ROWS

### 1.1. Текущая + предыдущая строка

**Задача:** Показать скользящую сумму за текущий и предыдущий день.

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS running_sum_2days
FROM sales
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | running_sum_2days |
|--------------|------------|--------|-------------------|
| Мышь         | 2024-01-01 | 30.00  | 30.00             |
| Мышь         | 2024-01-02 | 45.00  | 75.00             |
| Мышь         | 2024-01-03 | 50.00  | 95.00             |
| Мышь         | 2024-01-04 | 40.00  | 90.00             |
| Мышь         | 2024-01-05 | 55.00  | 95.00             |
| Мышь         | 2024-01-06 | 60.00  | 115.00            |
| Ноутбук      | 2024-01-01 | 100.00 | 100.00            |
| Ноутбук      | 2024-01-02 | 150.00 | 250.00            |
| Ноутбук      | 2024-01-03 | 120.00 | 270.00            |
| Ноутбук      | 2024-01-04 | 180.00 | 300.00            |
| Ноутбук      | 2024-01-05 | 200.00 | 380.00            |
| Ноутбук      | 2024-01-06 | 170.00 | 370.00            |

### 1.2. Скользящее среднее за 3 дня

**Задача:** Рассчитать среднее значение за текущий, предыдущий и следующий день.

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    AVG(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS moving_avg_3days
FROM sales
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | moving_avg_3days |
|--------------|------------|--------|------------------|
| Мышь         | 2024-01-01 | 30.00  | 37.50            |
| Мышь         | 2024-01-02 | 45.00  | 41.67            |
| Мышь         | 2024-01-03 | 50.00  | 45.00            |
| Мышь         | 2024-01-04 | 40.00  | 48.33            |
| Мышь         | 2024-01-05 | 55.00  | 51.67            |
| Мышь         | 2024-01-06 | 60.00  | 57.50            |
| Ноутбук      | 2024-01-01 | 100.00 | 125.00           |
| Ноутбук      | 2024-01-02 | 150.00 | 123.33           |
| Ноутбук      | 2024-01-03 | 120.00 | 150.00           |
| Ноутбук      | 2024-01-04 | 180.00 | 166.67           |
| Ноутбук      | 2024-01-05 | 200.00 | 183.33           |
| Ноутбук      | 2024-01-06 | 170.00 | 185.00           |

### 1.3. От начала до текущей строки

**Задача:** Показать накопительную сумму с начала периода.

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sum
FROM sales
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | cumulative_sum |
|--------------|------------|--------|----------------|
| Мышь         | 2024-01-01 | 30.00  | 30.00          |
| Мышь         | 2024-01-02 | 45.00  | 75.00          |
| Мышь         | 2024-01-03 | 50.00  | 125.00         |
| Мышь         | 2024-01-04 | 40.00  | 165.00         |
| Мышь         | 2024-01-05 | 55.00  | 220.00         |
| Мышь         | 2024-01-06 | 60.00  | 280.00         |
| Ноутбук      | 2024-01-01 | 100.00 | 100.00         |
| Ноутбук      | 2024-01-02 | 150.00 | 250.00         |
| Ноутбук      | 2024-01-03 | 120.00 | 370.00         |
| Ноутбук      | 2024-01-04 | 180.00 | 550.00         |
| Ноутбук      | 2024-01-05 | 200.00 | 750.00         |
| Ноутбук      | 2024-01-06 | 170.00 | 920.00         |

### 1.4. Полное окно

**Задача:** Показать общую сумму за весь период для каждой строки.

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS total_sum
FROM sales
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | total_sum |
|--------------|------------|--------|-----------|
| Мышь         | 2024-01-01 | 30.00  | 280.00    |
| Мышь         | 2024-01-02 | 45.00  | 280.00    |
| Мышь         | 2024-01-03 | 50.00  | 280.00    |
| Мышь         | 2024-01-04 | 40.00  | 280.00    |
| Мышь         | 2024-01-05 | 55.00  | 280.00    |
| Мышь         | 2024-01-06 | 60.00  | 280.00    |
| Ноутбук      | 2024-01-01 | 100.00 | 920.00    |
| Ноутбук      | 2024-01-02 | 150.00 | 920.00    |
| Ноутбук      | 2024-01-03 | 120.00 | 920.00    |
| Ноутбук      | 2024-01-04 | 180.00 | 920.00    |
| Ноутбук      | 2024-01-05 | 200.00 | 920.00    |
| Ноутбук      | 2024-01-06 | 170.00 | 920.00    |

---

## 2. Фреймы на основе RANGE

### 2.1. Разница между ROWS и RANGE

Создадим таблицу с дубликатами дат для демонстрации:

```sql
CREATE TABLE sales_with_duplicates (
    product_name VARCHAR(50),
    sale_date DATE,
    amount NUMERIC(10,2)
);

INSERT INTO sales_with_duplicates VALUES
    ('Ноутбук', '2024-01-01', 100),
    ('Ноутбук', '2024-01-01', 50),   -- Та же дата
    ('Ноутбук', '2024-01-02', 150),
    ('Ноутбук', '2024-01-03', 120);
```

**ROWS (физические строки):**
```sql
SELECT 
    product_name,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS rows_sum
FROM sales_with_duplicates
ORDER BY product_name, sale_date;
```

**Результат ROWS:**
| product_name | sale_date  | amount | rows_sum |
|--------------|------------|--------|----------|
| Ноутбук      | 2024-01-01 | 100.00 | 100.00   |
| Ноутбук      | 2024-01-01 | 50.00  | 150.00   |
| Ноутбук      | 2024-01-02 | 150.00 | 200.00   |
| Ноутбук      | 2024-01-03 | 120.00 | 270.00   |

**RANGE (логические группы):**
```sql
SELECT 
    product_name,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS range_sum
FROM sales_with_duplicates
ORDER BY product_name, sale_date;
```

**Результат RANGE:**
| product_name | sale_date  | amount | range_sum |
|--------------|------------|--------|-----------|
| Ноутбук      | 2024-01-01 | 100.00 | 150.00    |
| Ноутбук      | 2024-01-01 | 50.00  | 150.00    |
| Ноутбук      | 2024-01-02 | 150.00 | 150.00    |
| Ноутбук      | 2024-01-03 | 120.00 | 270.00    |

**Объяснение:** RANGE группирует строки с одинаковой датой и суммирует их вместе.

---

## 3. Фреймы на основе GROUPS (PostgreSQL 11+)

**Задача:** Работа с группами строк, имеющих одинаковое значение сортировки.

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS groups_sum
FROM sales_with_duplicates
ORDER BY product_name, sale_date;
```

**Результат:**
| product_name | sale_date  | amount | groups_sum |
|--------------|------------|--------|------------|
| Ноутбук      | 2024-01-01 | 100.00 | 150.00     |
| Ноутбук      | 2024-01-01 | 50.00  | 150.00     |
| Ноутбук      | 2024-01-02 | 150.00 | 300.00     |
| Ноутбук      | 2024-01-03 | 120.00 | 270.00     |

**Объяснение:** `GROUPS 1 PRECEDING` включает текущую группу (2024-01-01) и предыдущую группу (нет предыдущей для первой группы).

---

## 4. Функции положения с фреймами

### 4.1. FIRST_VALUE с разными фреймами

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS first_from_current_to_end
FROM sales
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | first_from_current_to_end |
|--------------|------------|--------|---------------------------|
| Мышь         | 2024-01-01 | 30.00  | 30.00                     |
| Мышь         | 2024-01-02 | 45.00  | 45.00                     |
| Мышь         | 2024-01-03 | 50.00  | 50.00                     |
| Мышь         | 2024-01-04 | 40.00  | 40.00                     |
| Мышь         | 2024-01-05 | 55.00  | 55.00                     |
| Мышь         | 2024-01-06 | 60.00  | 60.00                     |

### 4.2. LAST_VALUE с правильным фреймом

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_value_full_window
FROM sales
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | last_value_full_window |
|--------------|------------|--------|------------------------|
| Мышь         | 2024-01-01 | 30.00  | 60.00                  |
| Мышь         | 2024-01-02 | 45.00  | 60.00                  |
| Мышь         | 2024-01-03 | 50.00  | 60.00                  |
| Мышь         | 2024-01-04 | 40.00  | 60.00                  |
| Мышь         | 2024-01-05 | 55.00  | 60.00                  |
| Мышь         | 2024-01-06 | 60.00  | 60.00                  |
| Ноутбук      | 2024-01-01 | 100.00 | 170.00                 |
| Ноутбук      | 2024-01-02 | 150.00 | 170.00                 |
| Ноутбук      | 2024-01-03 | 120.00 | 170.00                 |
| Ноутбук      | 2024-01-04 | 180.00 | 170.00                 |
| Ноутбук      | 2024-01-05 | 200.00 | 170.00                 |
| Ноутбук      | 2024-01-06 | 170.00 | 170.00                 |

---

## 5. Сложные примеры

### 5.1. Поиск пиковых значений

**Задача:** Найти максимальное значение в окне из 3 дней.

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    MAX(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS max_in_window
FROM sales
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | max_in_window |
|--------------|------------|--------|---------------|
| Мышь         | 2024-01-01 | 30.00  | 45.00         |
| Мышь         | 2024-01-02 | 45.00  | 50.00         |
| Мышь         | 2024-01-03 | 50.00  | 50.00         |
| Мышь         | 2024-01-04 | 40.00  | 55.00         |
| Мышь         | 2024-01-05 | 55.00  | 60.00         |
| Мышь         | 2024-01-06 | 60.00  | 60.00         |
| Ноутбук      | 2024-01-01 | 100.00 | 150.00        |
| Ноутбук      | 2024-01-02 | 150.00 | 150.00        |
| Ноутбук      | 2024-01-03 | 120.00 | 180.00        |
| Ноутбук      | 2024-01-04 | 180.00 | 200.00        |
| Ноутбук      | 2024-01-05 | 200.00 | 200.00        |
| Ноутбук      | 2024-01-06 | 170.00 | 200.00        |

### 5.2. Процент от скользящей суммы

**Задача:** Рассчитать вклад каждой продажи в сумму последних 3 дней.

```sql
SELECT 
    product_name,
    sale_date,
    amount,
    SUM(amount) OVER w AS sum_3days,
    ROUND(100.0 * amount / SUM(amount) OVER w, 2) AS percent_contribution
FROM sales
WINDOW w AS (
    PARTITION BY product_name 
    ORDER BY sale_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)
ORDER BY product_name, sale_date;
```

**Результат:**

| product_name | sale_date  | amount | sum_3days | percent_contribution |
|--------------|------------|--------|-----------|---------------------|
| Мышь         | 2024-01-01 | 30.00  | 30.00     | 100.00              |
| Мышь         | 2024-01-02 | 45.00  | 75.00     | 60.00               |
| Мышь         | 2024-01-03 | 50.00  | 125.00    | 40.00               |
| Мышь         | 2024-01-04 | 40.00  | 135.00    | 29.63               |
| Мышь         | 2024-01-05 | 55.00  | 145.00    | 37.93               |
| Мышь         | 2024-01-06 | 60.00  | 155.00    | 38.71               |
| Ноутбук      | 2024-01-01 | 100.00 | 100.00    | 100.00              |
| Ноутбук      | 2024-01-02 | 150.00 | 250.00    | 60.00               |
| Ноутбук      | 2024-01-03 | 120.00 | 370.00    | 32.43               |
| Ноутбук      | 2024-01-04 | 180.00 | 450.00    | 40.00               |
| Ноутбук      | 2024-01-05 | 200.00 | 500.00    | 40.00               |
| Ноутбук      | 2024-01-06 | 170.00 | 550.00    | 30.91               |

---

## 6. Сравнение типов фреймов

Создадим специальный пример для наглядного сравнения:

```sql
CREATE TABLE test_frame (
    sort_key INT,
    value INT
);

INSERT INTO test_frame VALUES
    (1, 10),
    (1, 20),  -- Дубликат
    (2, 30),
    (2, 40),  -- Дубликат
    (3, 50);
```

**Сравнение ROWS, RANGE и GROUPS:**

```sql
SELECT 
    sort_key,
    value,
    SUM(value) OVER (ORDER BY sort_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_cumulative,
    SUM(value) OVER (ORDER BY sort_key RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_cumulative,
    SUM(value) OVER (ORDER BY sort_key GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS groups_cumulative
FROM test_frame
ORDER BY sort_key;
```

**Результат:**

| sort_key | value | rows_cumulative | range_cumulative | groups_cumulative |
|----------|-------|-----------------|------------------|-------------------|
| 1        | 10    | 10              | 30               | 30                |
| 1        | 20    | 30              | 30               | 30                |
| 2        | 30    | 60              | 100              | 100               |
| 2        | 40    | 100             | 100              | 100               |
| 3        | 50    | 150             | 150              | 150               |

**Объяснение различий:**

- **ROWS**: суммирует каждую физическую строку по порядку
- **RANGE**: суммирует все строки с одинаковым значением sort_key вместе
- **GROUPS**: суммирует целые группы строк с одинаковым значением sort_key

---

## Практические рекомендации

### 1. Выбор типа фрейма

| Сценарий | Рекомендуемый тип |
|----------|-------------------|
| Точное количество строк (скользящее окно) | `ROWS` |
| Логические диапазоны (например, за последние 7 дней по дате) | `RANGE` |
| Работа с дубликатами в ORDER BY | `GROUPS` или `RANGE` |
| Максимальная производительность | `ROWS` |

### 2. Фрейм по умолчанию

```sql
-- Эти запросы эквивалентны:
SELECT SUM(amount) OVER (PARTITION BY product_name ORDER BY sale_date) FROM sales;

SELECT SUM(amount) OVER (
    PARTITION BY product_name 
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) FROM sales;
```

### 3. Оптимизация производительности

```sql
-- Создайте индекс для ускорения ORDER BY
CREATE INDEX idx_sales_product_date ON sales(product_name, sale_date);

-- Используйте EXPLAIN для анализа
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    product_name,
    sale_date,
    SUM(amount) OVER (
        PARTITION BY product_name 
        ORDER BY sale_date
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS weekly_sum
FROM sales;
```

### 4. Типичные ошибки

**Ошибка 1:** Забыли про фрейм для LAST_VALUE

```sql
-- Неправильно (вернет текущую строку)
SELECT LAST_VALUE(amount) OVER (PARTITION BY product_name ORDER BY sale_date) FROM sales;

-- Правильно
SELECT LAST_VALUE(amount) OVER (
    PARTITION BY product_name 
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) FROM sales;
```

**Ошибка 2:** Использование RANGE с некорректным типом данных

```sql
-- RANGE с датами работает корректно
RANGE BETWEEN '7 days' PRECEDING AND CURRENT ROW

-- RANGE с числами
RANGE BETWEEN 10 PRECEDING AND CURRENT ROW  -- включит все строки с разницей <= 10
```

---

## Резюме

| Тип фрейма | Поведение | Типичное использование |
|------------|-----------|----------------------|
| `ROWS` | Фиксированное количество строк | Скользящее среднее за N дней |
| `RANGE` | Диапазон по значению | Продажи за последние 7 календарных дней |
| `GROUPS` | Группы равных значений | Агрегация по дням с несколькими записями |

**Ключевые выводы:**

- Фрейм определяет, какие строки участвуют в вычислении для текущей строки
- `ROWS` работает с физическими строками, `RANGE` — с логическими значениями
- Для `LAST_VALUE` и `NTH_VALUE` всегда указывайте полный фрейм
- Фрейм не влияет на функции `LAG` и `LEAD`
- При наличии дубликатов в `ORDER BY` результаты `ROWS` и `RANGE` могут существенно различаться
- Используйте `WINDOW` для переиспользования определения окна с фреймом
