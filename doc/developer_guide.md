---
title: "Developer guide"
date: 2021-11-18
draft: false
weight: 30
description: "Setting up for KCIDB development"
---
Hacking
-------

If you want to hack on the source code, install the package in the editable
mode with the `-e/--editable` option, and with "dev" extra included. E.g.:

    pip3 install --user --editable '.[dev]'

The latter installs kcidb executables which use the modules from the source
directory, and changes to them will be reflected immediately without the need
to reinstall. It also installs extra development tools, such as `flake8` and
`pylint`.

Then make sure your PATH includes the `~/.local/bin` directory, e.g. with:

    export PATH="$PATH":~/.local/bin

Guidelines
----------

### Commits

When posting PRs, please keep one logical change per commit, and do not have
more than one commit per a logical change. This makes it easier to review,
think about, and manipulate changes. This way if you want to revert a change,
you only need to revert a single commit. Same goes for merges across branches.

For example, if you have to do a similar change across multiple files, make it
a single commit. OTOH, if you're doing different changes on the same file, or
even the same line, make them separate commits.

Before asking for a review, please make sure the commits are in order, and
don't keep amendments made on top of the previous review as separate commits -
squash them.

Please use [imperative mood](https://en.wikipedia.org/wiki/Imperative_mood)
when writing commit messages, i.e. as if giving a command for something to
change. Keep subjects short and to the point, expand in the body of the commit
message. This makes it easier to read, understand, and manipulate commit
history. E.g. write "Add support for querying incidents", or even just
"Support querying incidents", but not "Supported querying incidents", and not
"Code to support querying incidents".

Use "tags" in commit subjects to make them shorter and to indicate the scope
of the change. E.g. instead of writing "Add documentation on commit
requirements", write "doc: Describe commit requirements". Look at commit
history of particular areas you're changing for reference on which tags to
use, so they're consistent.

### I/O data validation

When loading data into a database, the data should be "directly compatible"
with the database's I/O schema. "Directly compatible" (awkward term, I know)
with a schema means that the data adheres to a schema with the same major
version number, and the same, or lower minor number. That is, it can be
interpreted by the receiver without "upgrading". The
`kcidb.io.<SCHEMA_VERSION>.is_compatible_directly()` function checks for that.

When fetching I/O data from the database, it should match the database's
I/O schema exactly. It should not be blindly upgraded to the
currently-supported latest schema version by the database client or the
drivers, because we might want to load it up into another database, which is
also using an older schema, and you cannot "downgrade" I/O data, only "upgrade".

When submitting data through the client (`kcidb.Client`), or via a message
queue (`kcidb.mq.IOPublisher`, which is used internally by the client), it
should be valid according to the current or a previous KCIDB schema.

However, it's up to the receiving database on the other end of that message
queue to reject the data, if it's newer than the database's schema, and at the
moment we have no mechanism to report that situation. The submitters must be
made explicitly aware of which versions they can submit, for now.

The ORM and OO modules always deal with the latest I/O schema for simplicity.
