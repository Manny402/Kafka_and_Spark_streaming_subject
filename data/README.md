# Retail Rocket data

The raw Retail Rocket files are intentionally not committed to Git. They are
large (more than 1 GB in this working copy) and are not needed to review the
code.

Download the Retail Rocket recommender-system dataset from Kaggle:

<https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset>

Place these files in this directory:

```text
data/
├── events.csv
├── category_tree.csv
├── item_properties_part1.csv
└── item_properties_part2.csv
```

The notebook requires `events.csv` with this schema:

```text
timestamp,visitorid,event,itemid,transactionid
```

For a quick offline smoke test, run the notebook with `DATA_MODE=synthetic`;
it generates a small Retail-Rocket-like sample automatically. For the real
run, set `DATA_MODE=real` and verify that `data/events.csv` exists first.
