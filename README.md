<img src="https://kernelci.org/image/kernelci-horizontal-color.png"
     alt="KernelCI project logo"
     width="40%" />

KCIDB
=====

KCIDB is a package for submitting and querying Linux Kernel CI reports, coming
from independent CI systems, and for maintaining the service behind that.

See the collected results on [our dashboard](https://kcidb.kernelci.org/).
Write to [kernelci@groups.io](mailto:kernelci@groups.io) if you want to start
submitting results from your CI system, or if you want to receive automatic
notifications of arriving results.

Installation
------------

Kcidb requires Python v3.6 or later.

To install the package for the current user, run this command:

    pip3 install --user <SOURCE>

Where `<SOURCE>` is the location of the package source, e.g. a git repo:

    pip3 install --user git+https://github.com/kernelci/kcidb.git

or a directory path:

    pip3 install --user .

In any case, make sure your PATH includes the `~/.local/bin` directory, e.g.
with:

    export PATH="$PATH":~/.local/bin

Before you execute any of the tools make sure you have the path to your Google
Cloud credentials stored in the `GOOGLE_APPLICATION_CREDENTIALS` variable.
E.g.:

    export GOOGLE_APPLICATION_CREDENTIALS=~/.credentials.json

User guide
----------

### Submitting and querying

To submit records use `kcidb-submit`, to query records - `kcidb-query`.
Both use the same JSON schema on standard input and output respectively, which
can be displayed by `kcidb-schema`. You can validate the data without
submitting it using the `kcidb-validate` tool.

See [Submission HOWTO](SUBMISSION_HOWTO.md) for details.

### API

You can use the `kcidb` module to do everything the command-line tools do.

First, make sure you have the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable set and pointing at your Google Cloud credentials file. Then you can
create the client with `kcidb.Client(...)` and call its `submit(...)`
and `query(...)` methods.

You can find the I/O schema `in kcidb.io.schema.LATEST.json` and use
`kcidb.io.schema.validate()` to validate your I/O data.

See the source code for additional documentation.

Administrator guide
-------------------
See [ADMINISTRATOR_GUIDE.md](ADMINISTRATOR_GUIDE.md).

Developer guide
---------------

### Hacking

If you want to hack on the source code, install the package in the editable
mode with the `-e/--editable` option, and with "dev" extra included. E.g.:

    pip3 install --user --editable '.[dev]'

The latter installs kcidb executables which use the modules from the source
directory, and changes to them will be reflected immediately without the need
to reinstall. It also installs extra development tools, such as `flake8` and
`pylint`.

### Releasing

Before releasing make sure the README.md and SUBMISSION_HOWTO.md are up to
date.

To make a release tag the release commit with `v<NUMBER>`, where `<NUMBER>` is
the next release number, e.g. `v3`. The very next commit after the tag should
update the version number in `setup.py` to be the next one. I.e. continuing
the above example, it should be `4`.
