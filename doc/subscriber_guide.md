---
title: "Subscriber guide"
date: 2024-09-13
draft: false
weight: 30
description: "Subscribe to KCIDB email notifications"
---
Email notifications
---------------------

It is possible to generate email notifications with the KCIDB data submissions.
It needs to have a [subscription module][subscriptions], a script that will
create a custom report using [templates]. A list of subscribers would receive
the report via an email notification.

Currently, multiple subscriptions are active for receiving email reports
including `stable-rc` and `stable-rt` reports. The reports list useful data
such as build and test failures with architecture and build config information
reported by different CI systems.

Here is an example of such report:
```
Subject: KernelCI report for stable-rc: v5.10.225

OVERVIEW

        Builds: 34 passed, 1 failed

    Boot tests: 392 passed, 36 failed

    CI systems: broonie, maestro

REVISION

    Commit
        name: v5.10.225
        hash: b57d01c66f40ec96d8a5df6c2cdbd75e15e5f07c
    Checked out from
        https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux-stable-rc.git linux-5.10.y


BUILDS

    Failures
      -arm64 (virtconfig)
      Build detail: https://kcidb.kernelci.org/d/build/build?orgId=1&var-id=broonie:b57d01c66f40ec96d8a5df6c2cdbd75e15e5f07c-arm64-virtconfig
      CI system: broonie


BOOT TESTS

    Failures

      arm:(multi_v7_defconfig)
      -bcm2836-rpi-2-b
      CI system: maestro

      i386:(defconfig)
      -asus-CM1400CXA-dalboz
      -acer-cp514-2h-1160g7-volteer
      -hp-x360-12b-ca0010nr-n4020-octopus
      CI system: maestro

      x86_64:(cros://chromeos-5.10/x86_64/chromeos-amd-stoneyridge.flavour.config,
              x86_64_defconfig,
              cros://chromeos-5.10/x86_64/chromeos-intel-pineview.flavour.config)
      -lenovo-TPad-C13-Yoga-zork
      -minnowboard-turbot-E3826
      CI system: maestro

See complete and up-to-date report at:

    https://kcidb.kernelci.org/d/revision/revision?orgId=1&var-git_commit_hash=b57d01c66f40ec96d8a5df6c2cdbd75e15e5f07c&var-patchset_hash=


Tested-by: kernelci.org bot <bot@kernelci.org>

Thanks,
KernelCI team
```

Such reports are useful for developers and maintainers to keep track of new
failures with each new revision and can use these data for further required
actions.

[subscriptions]: https://github.com/kernelci/kcidb/tree/main/kcidb/monitor/subscriptions
[templates]: https://github.com/kernelci/kcidb/tree/main/kcidb/templates
