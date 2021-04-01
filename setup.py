#!/usr/bin/env python3
# Copyright (c) 2018 Red Hat, Inc. All rights reserved. This copyrighted
# material is made available to anyone wishing to use, modify, copy, or
# redistribute it subject to the terms and conditions of the GNU General
# Public License v.2 or later.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
"""Install kcidb using setuptools."""
import setuptools

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

setuptools.setup(
    name="kcidb",
    version="9",
    python_requires=">=3.6",
    author="kernelci.org",
    author_email="kernelci@groups.io",
    description="KCIDB = Linux Kernel CI reporting tools",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/kernelci/kcidb",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GPLv2+",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Topic :: Database :: Front-Ends",
    ],
    # Must match requirements.txt.
    # Let's make that easier, pylint: disable=line-too-long
    install_requires=[
        "google-cloud-bigquery",
        "google-cloud-pubsub<2.0.0dev",
        "google-cloud-firestore",
        "google-cloud-secret-manager<2.0.0dev",
        "jsonschema[format]",
        "requests",
        "pyyaml",
        "jinja2",
        "python-dateutil",
        "cached-property",
        "jq@git+https://github.com/spbnick/jq.py.git@1.1.2.post1",
        "kcidb-io@git+https://github.com/kernelci/kcidb-io.git@847363db6c382d2e811a4ba6da54e4c6cccee54e",  # noqa: E501
    ],
    extras_require=dict(
        dev=[
            "flake8",
            "pylint",
            "yamllint",
            "pytest",
        ],
    ),
    entry_points=dict(
        console_scripts=[
            "kcidb-schema = kcidb:schema_main",
            "kcidb-validate = kcidb:validate_main",
            "kcidb-upgrade = kcidb:upgrade_main",
            "kcidb-count = kcidb:count_main",
            "kcidb-merge = kcidb:merge_main",
            "kcidb-submit = kcidb:submit_main",
            "kcidb-query = kcidb:query_main",
            "kcidb-summarize = kcidb:summarize_main",
            "kcidb-describe = kcidb:describe_main",
            "kcidb-notify = kcidb:notify_main",
            "kcidb-db-init = kcidb.db:init_main",
            "kcidb-db-cleanup = kcidb.db:cleanup_main",
            "kcidb-db-load = kcidb.db:load_main",
            "kcidb-db-dump = kcidb.db:dump_main",
            "kcidb-db-query = kcidb.db:query_main",
            "kcidb-db-complement = kcidb.db:complement_main",
            "kcidb-mq-io-publisher-init = "
            "kcidb.mq:io_publisher_init_main",
            "kcidb-mq-io-publisher-cleanup = "
            "kcidb.mq:io_publisher_cleanup_main",
            "kcidb-mq-io-publisher-publish = "
            "kcidb.mq:io_publisher_publish_main",
            "kcidb-mq-io-subscriber-init = "
            "kcidb.mq:io_subscriber_init_main",
            "kcidb-mq-io-subscriber-cleanup = "
            "kcidb.mq:io_subscriber_cleanup_main",
            "kcidb-mq-io-subscriber-pull = "
            "kcidb.mq:io_subscriber_pull_main",
            "kcidb-oo-query = kcidb.oo:query_main",
            "kcidb-tests-validate = kcidb.tests:validate_main",
            "kcidb-monitor-spool-wipe = kcidb.monitor.spool:wipe_main",
        ]
    ),
    package_data={
        "": ["*.j2"],
    },
)
