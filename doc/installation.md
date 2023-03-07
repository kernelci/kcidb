---
title: "Installation"
date: 2021-11-18
draft: false
weight: 10
description: "How to install the KCIDB package"
---
KCIDB requires Python v3.6 or later.

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

If you're representing a submitting CI system accessing the KCIDB service, you
will get your credentials from us. Otherwise you will need to create a service
account in your Google Cloud project, and download its key file to act as the
credentials.
