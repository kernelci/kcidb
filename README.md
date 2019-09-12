KCIDB
=====

Kcidb is a package for entering and querying data to/from kernelci.org test
execution database.

Kcidb uses Google BigQuery for data storage. To be able to store or query
anything you need to create a BigQuery dataset.

Before you execute any of the tools make sure you have the path to your
BigQuery credentials stored in the GOOGLE_APPLICATION_CREDENTIALS variable.
E.g.:

    export GOOGLE_APPLICATION_CREDENTIALS=~/.bq.json

To initialize the dataset, execute `kcidb-init -d <DATASET>`, where
`<DATASET>` is the name of the dataset to initialize.

To create records use `kcidb-submit`, to query records - `kcidb-query`.

To cleanup the dataset (remove the tables) use `kcidb-cleanup`.
