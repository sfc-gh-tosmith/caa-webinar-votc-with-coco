/*=====================================================================
  VOC_PIPELINE — Full Project Setup Script
  
  Recreates the entire Voice of Customer analytics pipeline:
    1. Database & schemas
    2. File format & raw tables
    3. Data loading from stage
    4. Dynamic tables (AI-enriched)
    5. Semantic view
    6. Cortex Agent
=====================================================================*/

------------------------------------------------------------
-- 1. DATABASE & SCHEMAS
------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS VOC_LAB;
CREATE SCHEMA IF NOT EXISTS VOC_LAB.RAW;
CREATE SCHEMA IF NOT EXISTS VOC_LAB.ANALYTICS;

------------------------------------------------------------
-- 2. FILE FORMAT & RAW TABLES
------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT VOC_LAB.RAW.CSV_WITH_HEADER
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

CREATE OR REPLACE TABLE VOC_LAB.RAW.CUSTOMERS (
    CUSTOMER_ID    INTEGER,
    CUSTOMER_NAME  VARCHAR,
    EMAIL          VARCHAR,
    AGE_GROUP      VARCHAR,
    REGION         VARCHAR,
    LOYALTY_TIER   VARCHAR,
    SIGNUP_DATE    DATE
);

CREATE OR REPLACE TABLE VOC_LAB.RAW.TRANSACTIONS (
    TRANSACTION_ID   INTEGER,
    CUSTOMER_ID      INTEGER,
    PURCHASE_DATE    DATE,
    PRODUCT_CATEGORY VARCHAR,
    PRODUCT_NAME     VARCHAR,
    AMOUNT           NUMBER(10,2),
    QUANTITY         INTEGER
);

CREATE OR REPLACE TABLE VOC_LAB.RAW.CALL_TRANSCRIPTS (
    CALL_ID      INTEGER,
    CUSTOMER_ID  INTEGER,
    CALL_DATE    DATE,
    TRANSCRIPT   VARCHAR
);

------------------------------------------------------------
-- 3. LOAD DATA FROM STAGE
------------------------------------------------------------
COPY INTO VOC_LAB.RAW.CUSTOMERS
  FROM @AI_DEMOS.PUBLIC.VOTC_COCO_STAGE/customers.csv
  FILE_FORMAT = (FORMAT_NAME = 'VOC_LAB.RAW.CSV_WITH_HEADER');

COPY INTO VOC_LAB.RAW.TRANSACTIONS
  FROM @AI_DEMOS.PUBLIC.VOTC_COCO_STAGE/transactions.csv
  FILE_FORMAT = (FORMAT_NAME = 'VOC_LAB.RAW.CSV_WITH_HEADER');

COPY INTO VOC_LAB.RAW.CALL_TRANSCRIPTS
  FROM @AI_DEMOS.PUBLIC.VOTC_COCO_STAGE/call_transcripts.csv
  FILE_FORMAT = (FORMAT_NAME = 'VOC_LAB.RAW.CSV_WITH_HEADER');

------------------------------------------------------------
-- 4. DYNAMIC TABLE: ENRICHED CALL TRANSCRIPTS
--    Runs AI_SENTIMENT and AI_CLASSIFY over call data
------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE VOC_LAB.ANALYTICS.ENRICHED_CALL_TRANSCRIPTS
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT
    ct.CALL_ID,
    ct.CUSTOMER_ID,
    ct.CALL_DATE,
    ct.TRANSCRIPT,
    c.CUSTOMER_NAME,
    c.REGION,
    c.LOYALTY_TIER,
    AI_SENTIMENT(ct.TRANSCRIPT):categories[0]:sentiment::VARCHAR AS OVERALL_SENTIMENT,
    AI_CLASSIFY(
        ct.TRANSCRIPT,
        ['Order Status', 'Product Issue', 'Return or Exchange', 'Billing or Refund',
         'Shipping or Tracking', 'Loyalty or Rewards', 'Product Inquiry',
         'Cancellation', 'Feedback or Complaint', 'Website Issue']
    ):labels[0]::VARCHAR AS CALL_CATEGORY
FROM VOC_LAB.RAW.CALL_TRANSCRIPTS ct
LEFT JOIN VOC_LAB.RAW.CUSTOMERS c
    ON ct.CUSTOMER_ID = c.CUSTOMER_ID;

------------------------------------------------------------
-- 5. DYNAMIC TABLE: CUSTOMER INSIGHTS
--    Sentiment rollup, trend detection, spend metrics
------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE VOC_LAB.ANALYTICS.CUSTOMER_INSIGHTS
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = FULL
AS
WITH sentiment_scored AS (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        REGION,
        LOYALTY_TIER,
        CALL_ID,
        CALL_DATE,
        OVERALL_SENTIMENT,
        CALL_CATEGORY,
        TRANSCRIPT,
        CASE OVERALL_SENTIMENT
            WHEN 'positive' THEN 1.0
            WHEN 'neutral'  THEN 0.0
            WHEN 'mixed'    THEN 0.0
            WHEN 'negative' THEN -1.0
        END AS SENTIMENT_SCORE
    FROM VOC_LAB.ANALYTICS.ENRICHED_CALL_TRANSCRIPTS
),
customer_sentiment AS (
    SELECT
        CUSTOMER_ID,
        ANY_VALUE(CUSTOMER_NAME) AS CUSTOMER_NAME,
        ANY_VALUE(REGION) AS REGION,
        ANY_VALUE(LOYALTY_TIER) AS LOYALTY_TIER,
        COUNT(*) AS TOTAL_CALLS,
        ROUND(AVG(SENTIMENT_SCORE), 2) AS AVG_SENTIMENT_SCORE,
        SUM(IFF(OVERALL_SENTIMENT = 'negative', 1, 0)) AS NEGATIVE_CALL_COUNT
    FROM sentiment_scored
    GROUP BY CUSTOMER_ID
),
latest_call AS (
    SELECT *
    FROM sentiment_scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CALL_DATE DESC, CALL_ID DESC) = 1
),
latest_negative AS (
    SELECT
        CUSTOMER_ID,
        CALL_DATE AS LAST_NEGATIVE_DATE,
        CALL_CATEGORY AS LAST_NEGATIVE_CATEGORY,
        TRANSCRIPT AS LAST_NEGATIVE_TRANSCRIPT
    FROM sentiment_scored
    WHERE OVERALL_SENTIMENT = 'negative'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CALL_DATE DESC, CALL_ID DESC) = 1
),
spend_metrics AS (
    SELECT
        CUSTOMER_ID,
        COUNT(*) AS TOTAL_TRANSACTIONS,
        ROUND(SUM(AMOUNT), 2) AS TOTAL_SPEND,
        ROUND(AVG(AMOUNT), 2) AS AVG_SPEND_PER_TXN,
        MAX(PURCHASE_DATE) AS LAST_PURCHASE_DATE
    FROM VOC_LAB.RAW.TRANSACTIONS
    GROUP BY CUSTOMER_ID
)
SELECT
    cs.CUSTOMER_ID,
    cs.CUSTOMER_NAME,
    cs.REGION,
    cs.LOYALTY_TIER,
    cs.TOTAL_CALLS,
    cs.AVG_SENTIMENT_SCORE,
    lc.OVERALL_SENTIMENT AS LATEST_CALL_SENTIMENT,
    lc.CALL_DATE AS LATEST_CALL_DATE,
    lc.CALL_CATEGORY AS LATEST_CALL_CATEGORY,
    CASE
        WHEN lc.SENTIMENT_SCORE > cs.AVG_SENTIMENT_SCORE THEN 'TRENDING UP'
        WHEN lc.SENTIMENT_SCORE < cs.AVG_SENTIMENT_SCORE THEN 'TRENDING DOWN'
        ELSE 'STABLE'
    END AS SENTIMENT_TREND,
    cs.NEGATIVE_CALL_COUNT,
    ln.LAST_NEGATIVE_DATE,
    ln.LAST_NEGATIVE_CATEGORY,
    ln.LAST_NEGATIVE_TRANSCRIPT,
    sm.TOTAL_TRANSACTIONS,
    sm.TOTAL_SPEND,
    sm.AVG_SPEND_PER_TXN,
    sm.LAST_PURCHASE_DATE
FROM customer_sentiment cs
JOIN latest_call lc ON cs.CUSTOMER_ID = lc.CUSTOMER_ID
LEFT JOIN latest_negative ln ON cs.CUSTOMER_ID = ln.CUSTOMER_ID
LEFT JOIN spend_metrics sm ON cs.CUSTOMER_ID = sm.CUSTOMER_ID;

------------------------------------------------------------
-- 6. SEMANTIC VIEW
------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW VOC_LAB.ANALYTICS.VOC_SEMANTIC_VIEW

  TABLES (
    enriched_calls AS VOC_LAB.ANALYTICS.ENRICHED_CALL_TRANSCRIPTS
      PRIMARY KEY (CALL_ID)
      WITH SYNONYMS ('calls', 'call transcripts', 'support calls', 'interactions')
      COMMENT = 'Enriched call transcripts with AI-derived sentiment and category',

    customers AS VOC_LAB.ANALYTICS.CUSTOMER_INSIGHTS
      PRIMARY KEY (CUSTOMER_ID)
      WITH SYNONYMS ('customer insights', 'customer summary')
      COMMENT = 'Customer-level rollup with sentiment trends and spend metrics'
  )

  RELATIONSHIPS (
    calls_to_customers AS
      enriched_calls (CUSTOMER_ID) REFERENCES customers
  )

  FACTS (
    enriched_calls.sentiment_score AS
      CASE OVERALL_SENTIMENT
        WHEN 'positive' THEN 1.0
        WHEN 'neutral'  THEN 0.0
        WHEN 'mixed'    THEN 0.0
        WHEN 'negative' THEN -1.0
      END
      COMMENT = 'Numeric sentiment score: -1 (negative) to 1 (positive)',

    customers.avg_sentiment_fact AS AVG_SENTIMENT_SCORE
      COMMENT = 'Pre-computed average sentiment score per customer'
  )

  DIMENSIONS (
    enriched_calls.call_category AS CALL_CATEGORY
      WITH SYNONYMS = ('category', 'topic', 'issue type', 'complaint type')
      COMMENT = 'AI-classified call category',

    enriched_calls.overall_sentiment AS OVERALL_SENTIMENT
      WITH SYNONYMS = ('sentiment', 'mood', 'tone')
      COMMENT = 'AI-detected sentiment: positive, negative, neutral, or mixed',

    enriched_calls.call_date AS CALL_DATE
      COMMENT = 'Date of the support call',

    enriched_calls.customer_name AS enriched_calls.CUSTOMER_NAME
      WITH SYNONYMS = ('customer', 'caller', 'client')
      COMMENT = 'Customer name from the call',

    enriched_calls.region AS enriched_calls.REGION
      WITH SYNONYMS = ('area', 'geography')
      COMMENT = 'Customer region',

    enriched_calls.loyalty_tier AS enriched_calls.LOYALTY_TIER
      WITH SYNONYMS = ('tier', 'membership level')
      COMMENT = 'Customer loyalty tier',

    enriched_calls.transcript AS enriched_calls.TRANSCRIPT
      COMMENT = 'Full call transcript text',

    customers.customer_name_insight AS customers.CUSTOMER_NAME
      WITH SYNONYMS = ('customer', 'client name')
      COMMENT = 'Customer name',

    customers.sentiment_trend AS customers.SENTIMENT_TREND
      WITH SYNONYMS = ('trend', 'direction')
      COMMENT = 'Whether latest sentiment is trending up, down, or stable vs average',

    customers.latest_call_category AS customers.LATEST_CALL_CATEGORY
      COMMENT = 'Category of the most recent call',

    customers.latest_call_sentiment AS customers.LATEST_CALL_SENTIMENT
      COMMENT = 'Sentiment of the most recent call',

    customers.last_negative_category AS customers.LAST_NEGATIVE_CATEGORY
      WITH SYNONYMS = ('last complaint topic')
      COMMENT = 'Category of the most recent negative call'
  )

  METRICS (
    enriched_calls.total_calls AS COUNT(CALL_ID)
      WITH SYNONYMS = ('call count', 'number of calls', 'interactions')
      COMMENT = 'Total number of support calls',

    enriched_calls.avg_sentiment AS AVG(enriched_calls.sentiment_score)
      WITH SYNONYMS = ('average sentiment', 'mean sentiment')
      COMMENT = 'Average sentiment score across calls',

    enriched_calls.negative_call_count AS COUNT_IF(OVERALL_SENTIMENT = 'negative')
      WITH SYNONYMS = ('complaints', 'negative interactions')
      COMMENT = 'Number of calls with negative sentiment',

    enriched_calls.positive_call_count AS COUNT_IF(OVERALL_SENTIMENT = 'positive')
      WITH SYNONYMS = ('positive interactions')
      COMMENT = 'Number of calls with positive sentiment',

    customers.total_customers AS COUNT(customers.CUSTOMER_ID)
      COMMENT = 'Total number of customers',

    customers.avg_total_spend AS AVG(customers.TOTAL_SPEND)
      WITH SYNONYMS = ('average spend', 'mean spend')
      COMMENT = 'Average total spend per customer',

    customers.avg_transactions AS AVG(customers.TOTAL_TRANSACTIONS)
      COMMENT = 'Average number of transactions per customer',

    customers.total_negative_calls AS SUM(customers.NEGATIVE_CALL_COUNT)
      COMMENT = 'Sum of negative calls across customers'
  )

  COMMENT = 'Voice of Customer semantic view for analyzing call sentiment, categories, and customer health'

  AI_SQL_GENERATION 'When asked about complaints, filter for negative sentiment. When asked about recent calls, use the most recent CALL_DATE values. Round numeric values to 2 decimal places. If no time filter is specified, include all available data.';

------------------------------------------------------------
-- 7. CORTEX AGENT
------------------------------------------------------------
CREATE OR REPLACE AGENT VOC_LAB.ANALYTICS.VOC_AGENT
  COMMENT = 'Voice of Customer agent for querying call sentiment and customer insights'
  PROFILE = '{"display_name": "VOC Agent", "color": "blue"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: claude-4-sonnet

  orchestration:
    budget:
      seconds: 60
      tokens: 16000

  instructions:
    response: "Respond concisely with data-driven insights. Always include specific numbers. Format results as tables when showing multiple rows."
    orchestration: "Use the Analyst tool for all data questions about calls, sentiment, customers, categories, and spend."
    system: "You are a Voice of Customer analytics assistant. You help analyze support call sentiment, customer satisfaction trends, and identify areas for improvement."

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "Analyst"
        description: "Queries call transcript data, sentiment analysis, customer insights, and spend metrics"

  tool_resources:
    Analyst:
      semantic_view: "VOC_LAB.ANALYTICS.VOC_SEMANTIC_VIEW"
      execution_environment:
        type: "warehouse"
        warehouse: "COMPUTE_WH"
  $$;
