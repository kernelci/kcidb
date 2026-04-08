---
title: "Administrator guide"
date: 2021-11-18
draft: false
weight: 50
description: "Deploying, maintaining, and upgrading a KCIDB service installation"
---

Architecture
-----

Kcidb infrastructure is mostly based on Google Cloud services at the moment:

```mermaid
flowchart LR
    %% ────────────────────────────────────────────
    %% Hosts
    %% ────────────────────────────────────────────
    subgraph Hosts
        kcidb_grafana["Webserver<br/>kcidb-grafana"]
        kcidb_query["Client<br/>kcidb-query"]
    end

    %% ────────────────────────────────────────────
    %% Google Cloud Project
    %% ────────────────────────────────────────────
    subgraph "Google Cloud Project"
        subgraph CloudSQL
            kcidb_XX["kcidb_XX<br/>checkouts<br/>builds<br/>tests"]
        end

    subgraph "Cloud Functions"
        kcidb_updated["kcidb_updated"]
        kcidb_spool_notifications["kcidb_spool_notifications"]
        kcidb_send_notification["kcidb_send_notification"]
    end

        subgraph Firestore
            notifications["notifications"]
        end

        subgraph "Secret Manager"
            kcidb_smtp_password["kcidb_smtp_password"]
        end

        subgraph Gmail
            bot["bot\@kernelci.org"]
        end
    end

    %% ────────────────────────────────────────────
    %% Azure Project
    %% ────────────────────────────────────────────
    subgraph "Azure Project"
        kcidb_rest["kcidb_rest"]
        spool((SPOOL))
        kcidb_ingester["kcidb_ingester"]
        kcidb_logspec["kcidb_logspec"]
        kcidb_webdashboard["kcidb_webdashboard"]
    end

    %% ────────────────────────────────────────────
    %% External trigger
    %% ────────────────────────────────────────────
    kcidb_submit(["kcidb-submit, external submitters"])

    %% SPOOL object
    

    %% ────────────────────────────────────────────
    %% Flows
    %% ────────────────────────────────────────────
    kcidb_XX --> kcidb_grafana
    kcidb_XX --> kcidb_query
    kcidb_XX --> kcidb_webdashboard

    kcidb_submit --> kcidb_updated
    kcidb_updated --> kcidb_spool_notifications
    kcidb_spool_notifications --> notifications
    notifications --> kcidb_send_notification
    kcidb_smtp_password --> kcidb_send_notification
    kcidb_send_notification --> bot

    %% New Azure path
    kcidb_submit --> kcidb_rest
    kcidb_rest --> spool
    spool --> kcidb_ingester
    kcidb_ingester --> kcidb_XX
    kcidb_XX --> kcidb_logspec
    kcidb_logspec --> spool
```

CloudSQL(PostgreSQL) stores the report dataset and serves it to Grafana dashboards hosted
on kcidb.kernelci.org, as well as to any clients invoking `kcidb-query` or
using the kcidb library to query the database.

In the new architecture, client submissions are handled by the `kcidb` REST API, which
is hosted on Azure. The `kcidb` REST API available at `https://db.kernelci.org/`
is responsible for receiving client submissions and forwarding them to the `spool` directory.
The `spool` directory is a temporary storage area where submissions are
held before being processed. The `kcidb` REST API also handles
authentication and authorization of clients, ensuring that only
authorized users can submit data.

The `spool` directory is monitored by the `kcidb_ingester`, which is a
background process that continuously checks the `spool` directory for new
submissions. When a new submission is detected, the `kcidb_ingester`
processes it (including parsing and validating the data) and then
inserts it into the PostgreSQL database. The `kcidb_ingester` process
ensures that the data is properly formatted and adheres to the expected
schema before being stored in the database. This helps maintain data integrity
and consistency within the system.

In case of validation errors, the `kcidb_ingester` process logs the errors and
moves the submission to the `failed` directory for further investigation.

The `kcidb_logspec` process is responsible for identifying and creating
issues and incidents based on the data stored in the PostgreSQL database and 
external log artifacts. It analyzes the data and generates reports that can be
used to identify trends, issues, and potential areas for improvement within the
KernelCI ecosystem.

`kcidb_webdashboard` is a web-based interface that provides users with
access to the data stored in the PostgreSQL database. It allows users to
query and visualize the data, making it easier to analyze and understand
the results of the KernelCI tests. The web dashboard is designed to be
user-friendly and provides various features such as filtering, sorting,
and searching for specific data points. This enables users to quickly
find the information they need and gain insights into the results of the
KernelCI tests.
The `kcidb_webdashboard` is hosted on Azure and is accessible via a web
browser at URL `https://dashboard.kernelci.org/`.

### New Architecture transition

The new architecture is designed to improve the efficiency and reliability of
the KernelCI data submission and processing pipeline, reduce costs,
and make it independent of Google Cloud services (and any other cloud provider).
The architecture is more modular and flexible, allowing for
easier integration with other systems and services. It also provides a
more robust and scalable solution for handling the growing volume of
KernelCI data submissions.

A major feature of the new architecture is the ability to create self-hosted
installations of KernelCI, which can be deployed on any cloud provider or on-premises.

### Caching System

```mermaid
flowchart TB
    A[CI System] -->|Publish I/O data| B(kcidb_submit)

    subgraph Data Submission
        B(kcidb_submit) -->|Publish URLs| D(kcidb_updated_urls)
    end

    subgraph Caching System
        D(kcidb_updated_urls)-->|Push URLs| E([kcidb_cache_urls])
        E(["kcidb_cache_urls()"])-->|Store objects| F[(GCS bucket)]
    end

    subgraph Request Handling
        F[(GCS bucket)]<--> G(["kcidb_cache_redirect()"])
    end

    G(["kcidb_cache_redirect()"])-->|Cache found| I{{Bucket URL}}
    G(["kcidb_cache_redirect()"])-->|Cache not found| H{{Original URL}}
    J[User browser]-->|HTTP GET| G(["kcidb_cache_redirect()"])

    subgraph Legend
    K([Cloud Functions])
    end
```

1. **Publishing Initial Data**: The CI System initiates the process by submitting I/O data via the KCIDB submission system.

2. **URLs Extraction**:
The submission processing extracts URLs and publishes them to the `kcidb_updated_urls` queue.

3. **URL Processing and Cache Logic**: The `kcidb_cache_urls()` function receives the URLs from the `kcidb_updated_urls` queue, fetches the files from their locations, and stores them in the Google Cloud Storage bucket.

### Cache Request Handling

1. **User File Request**: When a user requests a file then that request is directed to the `kcidb_cache_redirect()` cloud function. This function serves as the entry point for processing user requests and initiating the cache lookup process.

2. **Cache Lookup**: The `kcidb_cache_redirect()` function interacts with the Google Cloud Storage bucket to find and serve the file from there, if its available.

3. **File Availability Check**: If the requested file is found within the cache storage, the `kcidb_cache_redirect()` function performs a redirection to the location of the file within the Google Cloud Storage Bucket and serve it to the user.

4. **Fallback Mechanism**: In cases where the requested file is not present within the cache storage, then `kcidb_cache_redirect()` function redirects the user to the original URL from which the file was initially requested.

Setup
-----

To setup and manage most of Google Cloud services you will need the `gcloud`
tool, which is a part of Google Cloud SDK. You can install it and create a
Google Cloud Project by following one of the [official quickstart
guides](https://cloud.google.com/sdk/docs/quickstarts). The instructions below
assume the created project ID is `kernelci-production` (yours likely won't be).

Authenticate the gcloud tool with your Google account:

    gcloud auth login

Select the project you just created:

    gcloud config set project kernelci-production

Create an administrative service account (`kernelci-production-admin` from here on):

    gcloud iam service-accounts create kernelci-production-admin

Grant the administrative service account the project owner permissions:

    gcloud projects add-iam-policy-binding kernelci-production \
           --member "serviceAccount:kernelci-production-admin@kernelci-production.iam.gserviceaccount.com" \
           --role "roles/owner"

Generate the account key file (`kernelci-production-admin.json` here):

    gcloud iam service-accounts keys create kernelci-production-admin.json \
           --iam-account kernelci-production-admin@kernelci-production.iam.gserviceaccount.com

NOTE: This key allows anyone to do **anything** with the specified
      Google Cloud project, so keep it safe.

Select the account key for use with Google Cloud API (which kcidb uses):

    export GOOGLE_APPLICATION_CREDENTIALS=`pwd`/kernelci-production-admin.json

Clone kcidb repository, checkout the revision you want to deploy, and install
kcidb as described in [README.md](README.md).

### Additional Dependencies

While deploying you may also need to install `psql`. It is a command-line
tool for interacting with the PostgreSQL database. It is used to manage
and manipulate data stored in a PostgreSQL database, as well as to
administer and configure the database itself.

For Debian-based distros like Ubuntu:
   
    apt-get install postgresql-client

For Fedora-based distros like CentOS and RHEL:

    sudo dnf install postgresql

Verify the psql installation: 
   
    psql --version

Once the installation is complete, you can use `psql` for the
deployment of the project.

### Deploying

Use the `cloud` tool located in the root of kcidb repository to deploy the base
setup. Assuming your current directory is the repository root, this command:

    ./cloud deploy PROJECT NAMESPACE VERSION

will create an installation in Google Cloud project `PROJECT`, all object
names prefixed with `NAMESPACE_`, and BigQuery dataset with name ending with
the `VERSION` number.

You can rerun the command any number of times to have the installation
updated.

You will be asked to enter the SMTP user password if the corresponding secret
is not found. You can use the `--smtp-password-file` option to supply it
explicitly when creating the secret, or to update the secret on successive
runs.

Use the `--smtp-to-addrs` to specify comma-separated email addresses to send
reports to, instead of sending them to their intended recipients. The
recipients are overridden to `kernelci-results-staging@groups.io` by default.
Specify an empty string to configure sending to actual recipients.

Use the `--log-level` option to specify the (Python-style) logging level of
Cloud Functions.

Use the `--submitter` option to specify the name of a service account which
should be permitted to submit data to KCIDB. Repeat to add more accounts.

Use the `-v/--verbose` option to make the tool report the progress of the
installation.

Finally, you can use the `-s/--sections` option to specify and extended shell
glob matching the "sections" of the installation to restrict the deployment
to. See the output of `cloud list-sections` for a list of available sections.

### Withdrawing

Use the `cloud` tool located in the root of kcidb repository to "withdraw"
(remove) the base setup. Assuming your current directory is the repository
root, this command:

    ./cloud withdraw PROJECT NAMESPACE VERSION

Will remove the installed objects with names prefixed with `NAMESPACE_`, and
the BigQuery dataset with name ending with the `VERSION` number from Google
Cloud project `PROJECT`.

You can use the `-v/--verbose` and `-s/--sections` similarly to `cloud deploy`
command, adding `--submitter` options will remove permissions to submit for
the specified service accounts.

### Grafana

See
[kcidb-grafana README.md](https://github.com/kernelci/kcidb-grafana/#setup)
for setup instructions.

### CI System Accounts

Each submitting/querying CI system needs to have a service account created,
permissions assigned, and the account key generated. Below is an example for
a CI system called "CKI" having account named "kernelci-production-ci-cki" created.

Create the service account:

    gcloud iam service-accounts create kernelci-production-ci-cki

Generate the account key file (`kernelci-production-ci-cki.json` here) for use by
the CI system:

    gcloud iam service-accounts keys create kernelci-production-ci-cki.json \
           --iam-account kernelci-production-ci-cki@kernelci-production.iam.gserviceaccount.com

Grant the account permissions for the BigQuery dataset and the submission
queue using the "cloud" tool located in the root of the kcidb repository:

    ./cloud deploy kernelci-production "" 0 --section submitters \
                 --submitter kernelci-production-ci-cki

Upgrading
---------

### BigQuery

To upgrade the dataset schema, do the following.

1. Authenticate to Google Cloud with the key file (`~/.kernelci-bq.json`
   here):

        gcloud auth activate-service-account --key-file ~/.kernelci-bq.json

   or login with your credentials (entered via a browser window):

        gcloud auth login

2. Create a new dataset (`kernelci02` in project `kernelci` here) with the new
   schema:

        bq mk --project_id=kernelci kernelci02
        # Using new-schema kcidb
        kcidb-db-init -d bigquery:kernelci02

3. Switch all data submitters to using new-schema kcidb and the newly-created
   dataset.

4. Create a new dataset with the name of the old one (`kernelci01` here), but
   with `_archive` suffix, using the old-schema kcidb:

        # Using old-schema kcidb
        kcidb-db-init -d bigquery:kernelci01_archive

5. Using BigQuery management console, shedule copying the old dataset to the
   created dataset. When that is done, remove the old dataset.

6. Transfer data from the copy of the old dataset (named `kernelci01_archive`
   here) to the new dataset (named `kernelci02` here) using old-schema
   `kcidb-db-dump` and new-schema `kcidb-db-load`.

        # Using old-schema kcidb
        kcidb-db-dump -d bigquery:kernelci01_archive > kernelci01_archive.json
        # Using new-schema kcidb
        kcidb-db-load -d bigquery:kernelci02 < kernelci01_archive.json
