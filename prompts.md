# Lab Prompts — Voice of the Customer

---

## Prompt 1 — Load the Data

I have three CSV files sitting on a Snowflake internal stage called @votc_files — customers.csv, transactions.csv, and call_transcripts.csv. Can you help me get this data into Snowflake? Create a database called VOC_LAB with a RAW schema, figure out the right table structures by looking at the files, load everything in, and then just do a quick sanity check — row counts and a few sample rows from each table so I know everything landed correctly.

---

## Prompt 2 — Enrich the Transcript Data with Cortex AI

Alright, now that we have our raw data in VOC_LAB.RAW, I want to start pulling actual insights out of the text. In a new schema called ANALYTICS, build a Dynamic Table that runs Cortex AI functions over the data — use SNOWFLAKE.CORTEX.SENTIMENT on the support call text, AI_CLASSIFY to bucket tickets into categories (you may have to use ai_agg to figure out the possible categories), and AI_EXTRACT to pull out product name. Set the target lag to 1 minute so it stays fresh. Show me a preview of what the enriched data looks like when it's done.

---

## Prompt 3 — Customer-level Analytics table

Now I want you to create a dynamic table that reads from the call analytics table and shows me the average sentiment for each customer and if their most recent interaction was more positive or more negative than average. It should also tell us what that most recent negative interaction was about so that we can easily flag it. It should also show some metrics about that customer, like their average spend, total amount of transactions, etc.

---

## Prompt 4 — Move the Pipeline into dbt

Now I want to take what we just built and put it into a dbt project so it's version controlled, tested, and documented. Can you initialize a dbt project called voc_pipeline and recreate the enrichment/analytics models. Add some basic tests too, like not_null on the primary keys and accepted_values on whatever categories AI_CLASSIFY is outputting. Wire up the RAW tables as dbt sources and then run dbt build so we can see everything passing. The goal is to have the exact same enriched output, just now living in a real pipeline we can commit to Git.

---

## Prompt 5 — Build the Agent

Last step — let's make all of this actually conversational. First, create a Semantic View that will help us answer questions like "Which call category has the lowest average sentiment?" and "Who are our top 10 most positive-interaciton customers?". Then build a Cortex Agent called VOC_AGENT that's hooked up to that semantic view. Once it's running, test it with these questions: "What are the most common complaints this month?", "Which product has the worst customer sentiment?", and "Summarize recent calls billing." Show me what it comes back with.
