KCIDB
=====

Kcidb is a package for entering and querying data to/from kernelci.org test
execution database.

Setup
-----

To install the package for the current user, run this command:

    pip3 install --user <SOURCE>

Where `<SOURCE>` is the location of the package source, e.g. a git repo:

    pip3 install --user git+https://github.com/spbnick/kcidb.git

or a directory path:

    pip3 install --user .

If you want to hack on the source code, install the package in the editable
mode with the `-e/--editable` option, and with "dev" extra included. E.g.:

    pip3 install --user --editable '.[dev]'

The latter installs kcidb executables which use the modules from the source
directory, and changes to them will be reflected immediately without the need
to reinstall. It also installs extra development tools, such as `flake8` and
`pylint`.

In any case, make sure your PATH includes the `~/.local/bin` directory, e.g.
with:

    export PATH="$PATH":~/.local/bin

Usage
-----
Kcidb uses Google BigQuery for data storage. To be able to store or query
anything you need to create a BigQuery dataset.

Before you execute any of the tools make sure you have the path to your
BigQuery credentials stored in the `GOOGLE_APPLICATION_CREDENTIALS` variable.
E.g.:

    export GOOGLE_APPLICATION_CREDENTIALS=~/.bq.json

To initialize the dataset, execute `kcidb-init -d <DATASET>`, where
`<DATASET>` is the name of the dataset to initialize.

To submit records use `kcidb-submit`, to query records - `kcidb-query`.
Both use the same JSON schema on standard input and output respectively, which
can be displayed by `kcidb-schema`.

To cleanup the dataset (remove the tables) use `kcidb-cleanup`.
