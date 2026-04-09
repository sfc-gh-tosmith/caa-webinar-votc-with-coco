# Voice of the Customer — Sample Data

Sample retail customer data for demonstrating Snowflake Cortex AI analytics (sentiment analysis, topic classification, structured extraction) on unstructured customer interactions.

## Files

| File | Rows | Description |
|------|------|-------------|
| `customers.csv` | 50 | Customer profiles with loyalty tier, region, and demographics |
| `transactions.csv` | 873 | Purchase history across 7 product categories |
| `call_transcripts.csv` | 175 | Customer service call transcripts |

## Data characteristics

- Every customer has at least 10 transactions and 2 call transcripts
- Higher-tier customers tend to have more transactions
- Call transcripts contain a realistic mix of positive, neutral, and negative interactions covering shipping, billing, product quality, returns, and general inquiries
