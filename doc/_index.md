---
title: "KCIDB"
date: 2023-08-11
description: "KernelCI Database service and tools"
---

[KCIDB](https://github.com/kernelci/kcidb) is a package for submitting and
querying Linux Kernel CI reports, coming from independent CI systems, and for
maintaining the service behind that.

See the collected results on [our dashboard](https://kcidb.kernelci.org/).
Write to [kernelci@lists.linux.dev](mailto:kernelci@lists.linux.dev) if you
want to start submitting results from your CI system, or if you want to
receive automatic notifications of arriving results.

See the [Tests Catalog](https://github.com/kernelci/kcidb/blob/main/tests.yaml)
file with the list of test identifiers used by KCIDB.

## CI Services contributing to KCIDB

The are a number of [CI Services in
production](https://github.com/orgs/kernelci/projects/16) contributing to
KCIDB, including:

* [KernelCI native tests](https://linux.kernelci.org/job/)
* [Red Hat CKI](https://cki-project.org/)
* [Google syzbot](https://syzkaller.appspot.com/)
* [Linaro Tuxsuite](https://tuxsuite.com/)
* [ARM](https://arm.com)
* [Gentoo GKernelCI](https://github.com/GKernelCI/GBuildbot)

### Red Hat CKI

This is Red Hat’s kernel CI service.  They run tests on a variety of
enterprise class machines.  This service is used to test the kernel package
in the Red Hat Enterprise Linux (RHEL) product.

* See [Contributing to
  CKI](https://cki-project.org/docs/test-maintainers/onboarding/) for
  instructions to add tests
* See [Contributing a submaintainer
  tree](https://cki-project.org/docs/user_docs/onboarding/) for instructions to
  test a git branch
* You can find more details at the [cki-project.org](https://cki-project.org)
* Contact: Send questions to **cki-project@redhat.com**
