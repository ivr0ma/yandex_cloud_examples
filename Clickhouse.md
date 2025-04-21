### 1. Создание таблицы `order_items` 
```sql
CREATE TABLE db1.order_items
(
    item_id UInt64,
    order_id UInt64,
    product_name String,
    product_price Decimal(10, 2),
    quantity UInt32
)
ENGINE = MergeTree()
ORDER BY (order_id, item_id);
```

#### 2. Вставка данных
```sql
INSERT INTO db1.order_items VALUES
(1, 1001, 'Smartphone', 600.0, 2),
(2, 1002, 'Laptop', 999.5, 1),
(3, 1004, 'Monitor', 300.0, 2),
(4, 1004, 'Keyboard', 50.0, 1),
(5, 1007, 'Mouse', 25.0, 2),
(6, 1010, 'Laptop', 799.0, 1),
(7, 1019, 'Laptop', 1100.0, 2),
(8, 1020, 'Speaker', 185.5, 3),
(9, 1009, 'Tablet', 500.0, 2),
(10, 1011, 'PhoneCase', 20.0, 3),
(11, 1012, 'GamingConsole', 650.0, 3),
(12, 1013, 'Book', 15.0, 10),
(13, 1014, 'Smartwatch', 300.0, 1),
(14, 1015, 'Monitor', 300.0, 2),
(15, 1015, 'Keyboard', 50.0, 1),
(16, 1016, 'Camera', 250.0, 2);
```

#### 3. Проверка данных
```sql
SELECT * FROM db1.order_items ORDER BY order_id LIMIT 10;
```

### 1. Создание таблицы `orders`
```sql
CREATE TABLE db1.orders
(
    order_id UInt64,
    user_id UInt64,
    order_date DateTime,
    total_amount Decimal(10, 2),
    payment_status String
)
ENGINE = MergeTree()
ORDER BY (order_id);
```

#### 2. Вставка данных
```sql
INSERT INTO db1.orders VALUES
(1001, 10, '2023-03-01 10:00:00', 1200.0, 'paid'),
(1002, 11, '2023-03-01 10:05:00', 999.5, 'pending'),
(1003, 10, '2023-03-01 10:10:00', 0.0, 'cancelled'),
(1004, 12, '2023-03-01 11:00:00', 1450.0, 'paid'),
(1005, 10, '2023-03-01 12:00:00', 500.0, 'paid'),
(1006, 13, '2023-03-02 09:00:00', 2100.0, 'paid'),
(1007, 14, '2023-03-02 09:30:00', 300.0, 'pending'),
(1008, 15, '2023-03-02 10:00:00', 450.0, 'paid'),
(1009, 10, '2023-03-02 10:15:00', 1000.0, 'pending'),
(1010, 11, '2023-03-02 11:00:00', 799.0, 'paid'),
(1011, 12, '2023-03-02 12:00:00', 120.0, 'cancelled'),
(1012, 13, '2023-03-03 08:00:00', 2000.0, 'paid'),
(1013, 15, '2023-03-03 09:00:00', 450.0, 'paid'),
(1014, 15, '2023-03-03 09:30:00', 899.99, 'paid'),
(1015, 14, '2023-03-03 10:00:00', 1350.0, 'paid'),
(1016, 10, '2023-03-03 11:00:00', 750.0, 'pending');
```

#### 3. Проверка данных
```sql
SELECT * FROM db1.orders ORDER BY order_date LIMIT 10;
```

### Примеры аналитических запросов:

#### Статистика по статусам оплаты:
```sql
SELECT 
    payment_status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_sum,
    AVG(total_amount) as avg_order
FROM 
    db1.orders
GROUP BY 
    payment_status;
```

#### Топ-5 пользователей по сумме заказов:
```sql
SELECT 
    user_id,
    SUM(total_amount) as total_spent,
    COUNT(*) as order_count
FROM 
    db1.orders
WHERE 
    payment_status = 'paid'
GROUP BY 
    user_id
ORDER BY 
    total_spent DESC
LIMIT 5;
```

#### Соединение с таблицей order_items:
```sql
SELECT 
    o.order_id,
    o.user_id,
    o.total_amount,
    COUNT(i.item_id) as items_count,
    SUM(i.product_price * i.quantity) as calculated_total
FROM 
    db1.orders o
JOIN 
    db1.order_items i ON o.order_id = i.order_id
GROUP BY 
    o.order_id, o.user_id, o.total_amount
ORDER BY 
    o.order_id;
```
