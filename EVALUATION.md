# EVALUATION.md

## 1. Test Accuracy Results Breakdown

| Query Type             | Total Tests | Passed | Failed | Accuracy |
|-------------------------|-------------|--------|--------|----------|
| **Simple Queries**      | 6           | 5      | 1      | 83%      |
| **Intermediate Queries**| 10          | 10     | 0      | 100%     |
| **Complex Queries**     | 5           | 2      | 3      | 40%      |
| **End-to-End Tests**    | 1           | 0      | 1      | 0%       |
| **Data Loader & Others**| 8           | 8      | 0      | 100%     |
| **TOTAL**               | 30          | 25     | 5      | **83%**  |

✅ Overall accuracy is **83%**, with the majority of failures concentrated in **complex reasoning queries** and **end-to-end SQL execution**.

---

## 2. Query Performance Metrics

- **Average execution time**: ~0.4s per query  
- **Fastest query**: <0.1s (simple SELECT from single table)  
- **Slowest query**: ~1.7s (multi-join with aggregation, category sales growth)  
- **Distribution**:
  - <0.5s: 70% queries
  - 0.5–1.0s: 20% queries
  - >1.0s: 10% queries

Overall performance is acceptable for interactive usage.  
Indexes on **foreign key columns** and **frequently filtered attributes** (e.g., `orders.order_date`, `products.category_id`) are critical.

---

## 3. Failed Queries Analysis

### ❌ `test_products_above_avg_margin_together`
- **Expected**: JOIN between `products`, `order_details`  
- **Got**: Standalone filter on `products` (unit price > avg)  
- **Cause**: Model ignored "ordered together" → produced no join.

### ❌ `test_yoy_sales_growth_per_category`
- **Expected**: Aggregation with `order_details`, `orders`, `categories`  
- **Got**: Partial query with category join, but missing sales calculation.  
- **Score**: 0.52 (<0.6 threshold).  
- **Cause**: Schema complexity & model prompt limitations.

### ❌ `test_customers_all_categories`
- **Expected**: Relational division query using `customers`, `orders`, `order_details`, `products`, `categories`  
- **Got**: `WHERE FALSE` → model fallback due to reasoning failure.  
- **Cause**: Model unable to infer "all categories" without explicit schema guidance.

### ❌ `test_sales_representatives`
- **Expected**: Filter on `employees.title = 'Sales Representative'`  
- **Got**: Query failed due to **Gemini API quota exhaustion**.  
- **Cause**: External rate-limiting, not pipeline logic.

### ❌ `test_end_to_end_simple_query`
- **Expected**: Simple SELECT with country filter.  
- **Got**: Query blocked by **Gemini quota error**.  
- **Cause**: Same as above — API quota exhaustion.

---

## 4. Database Optimization Opportunities

- **Indexes**:
  - `orders(order_date)`
  - `products(category_id)`
  - `order_details(order_id, product_id)`
  - `employees(title)`
- **GIN/Trigram indexes** for full-text queries (e.g., company name search).  
- **Materialized Views**:
  - Yearly sales per category
  - Top-N products/customers
- **Partitioning**:
  - `orders` by year (to optimize date range queries).

---

## 5. Lessons Learned & Challenges Faced

- **LLM limitations**: Struggles with relational division and multi-table joins without explicit schema hints.  
- **Quota issues**: Gemini API free tier frequently blocks test runs. Requires either billing or caching for reliability.  
- **Dynamic normalization** worked but **schema alignment** with Northwind was key for query quality.  
- **Test design**: Complex queries stress-test schema reasoning far beyond simple lookups.

---

## 6. Time Spent on Components

| Component                  | Hours |
|-----------------------------|-------|
| Dynamic normalization dev   | 8     |
| Database setup (Northwind)  | 5     |
| Text2SQL engine integration | 7     |
| Query validator & security  | 4     |
| Monitoring & metrics        | 3     |
| Test suite + debugging      | 6     |
| Documentation (README/EVAL) | 2     |
| **TOTAL**                   | **35** |

---

📊 **Final Evaluation**:  
- Project delivers **83% accuracy** overall.  
- **Intermediate queries are robust (100%)**, showing strength in mid-complexity analytics.  
- **Complex queries need schema-aware prompting & LLM fine-tuning** for production reliability.  
- With database tuning + prompt improvements, accuracy can realistically exceed **90%**.
