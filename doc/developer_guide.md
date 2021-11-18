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

Releasing
---------

Before releasing make sure the documentation files are up to date.

To make a release tag the release commit with `v<NUMBER>`, where `<NUMBER>` is
the next release number, e.g. `v3`. The very next commit after the tag should
update the version number in `setup.py` to be the next one. I.e. continuing
the above example, it should be `4`.
