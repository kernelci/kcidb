KCIDB
=====

Kcidb is a package for submitting and querying Linux Kernel CI reports,
and for maintaining the service behind that.

See the collected results on
[our dashboard](https://staging.kernelci.org:3000/).
Write to [kernelci@groups.io](mailto:kernelci@groups.io) if you want to start
submitting results from your CI system, or if you want to receive automatic
notifications of arriving results.

Installation
------------

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

Kcidb infrastructure is mostly based on Google Cloud services at the moment:

    === Hosts ===  =================== Google Cloud Project ========================

    ~~ Staging ~~                                                ~~~~ BigQuery ~~~~~
    kcidb-grafana <---------------------------------------------  . kernelciXX .
                                                                 :   revisions  :
    ~~ Client ~~~                                                :   builds     :
    kcidb-query <----------------------------------------------- :   tests      :
                                                                  ''''''''''''''
                    ~~ Pub/Sub ~~   ~~~~ Cloud Functions ~~~~            ^
    kcidb-submit -> kcidb_new ----> kcidb_load_message ------------------'
                                        |
                          .-------------'
                          v                                      ~~~~ Firestore ~~~~
                    kcidb_loaded -> kcidb_spool_notifications -> notifications
                                                                       |
                                               .-----------------------'
                                               |
                                               v                 ~ Secret Manager ~~
                                    kcidb_send_notification <--- kcidb_smtp_password
                                               |
                                               |                 ~~~~~~ GMail ~~~~~~
                                               `---------------> bot@kernelci.org

BigQuery stores the report dataset and serves it to Grafana dashboards hosted
on staging.kernelci.org, as well as to any clients invoking `kcidb-query` or
using the kcidb library to query the database.

Whenever a client submits reports, either via `kcidb-submit` or the kcidb
library, they go to a Pub/Sub message queue topic named `kcidb_new`, then to
the `kcidb_load_message` "Cloud Function", which loads the data to the
BigQuery dataset, and then pushes it to `kcidb_loaded` topic.

That topic is watched by `kcidb_spool_notifications` function, which picks up
the data, generates report notifications, and stores them in a Firestore
collection named `notifications`.

The last "Cloud Function", `kcidb_send_notification`, picks up the created
notifications from the Firestore collection, and sends them out through GMail,
using the `bot@kernelci.org` account, authenticating with the password stored
in `kcidb_smtp_password` secret, within Secret Manager.

### Setup

To setup and manage most of Google Cloud services you will need the `gcloud`
tool, which is a part of Google Cloud SDK. You can install it and create a
Google Cloud Project by following one of the [official quickstart
guides](https://cloud.google.com/sdk/docs/quickstarts). The instructions below
assume the created project ID is `kernelci-project` (yours likely won't be).

Authenticate the gcloud tool with your Google account:

    gcloud auth login

Select the project you just created:

    gcloud config set project kernelci-project

Create an administrative service account (`kernelci-project-admin` from here on):

    gcloud iam service-accounts create kernelci-project-admin

Grant the administrative service account the project owner permissions:

    gcloud projects add-iam-policy-binding kernelci-project \
           --member "serviceAccount:kernelci-project-admin@kernelci-project.iam.gserviceaccount.com" \
           --role "roles/owner"

Generate the account key file (`kernelci-project-admin.json` here):

    gcloud iam service-accounts keys create kernelci-project-admin.json \
           --iam-account kernelci-project-admin@kernelci-project.iam.gserviceaccount.com

NOTE: This key allows anyone to do **anything** with the specified
      Google Cloud project, so keep it safe.

Select the account key for use with Google Cloud API (which kcidb uses):

    export GOOGLE_APPLICATION_CREDENTIALS=`pwd`/kernelci-project-admin.json

Install kcidb as described above.

#### BigQuery

Create a BigQuery dataset (`kernelci03` here):

    bq mk kernelci03

Check it was created successfully:

    bq ls

Initialize the dataset:

    kcidb-db-init -d kernelci03

#### Pub/Sub

Enable the Pub/Sub API:

    gcloud services enable pubsub.googleapis.com

Create the `kernelci_new` topic:

    kcidb-mq-publisher-init -p kernelci-project -t kernelci_new

Create the `kernelci_loaded` topic:

    kcidb-mq-publisher-init -p kernelci-project -t kernelci_loaded

#### Firestore

Create a **native** Firestore database by following [the quickstart
guide](https://cloud.google.com/firestore/docs/quickstart-servers).


Enable the Firestore API:

    gcloud services enable firestore.googleapis.com

#### Secret Manager

Enable the Secret Manager API:

    gcloud services enable secretmanager.googleapis.com

Add the `kcidb_smtp_password` secret containing the GMail password (here
`PASSWORD`) for bot@kernelci.org:

    echo -n 'PASSWORD' | gcloud secrets create kcidb_smtp_password \
                                --replication-policy automatic \
                                --data-file=-

NOTE: For a more secure way specify a file with the secret to the
      `--data-file` option instead.

#### Cloud Functions

Requires all the services above setup first.

Enable the Cloud Functions API:

    gcloud services enable cloudfunctions.googleapis.com

Allow the default Cloud Functions account access to the SMTP password:

    gcloud secrets add-iam-policy-binding kcidb_smtp_password \
           --role roles/secretmanager.secretAccessor \
           --member serviceAccount:kernelci-project@appspot.gserviceaccount.com

Download and unpack, or clone the kcidb version being deployed, and change
into the source directory. E.g.:

    git clone https://github.com/kernelci/kcidb.git
    cd kcidb

Make sure the functions' environment variables specify the setup correctly,
amend if not:

    cat main.env.yaml

Deploy the functions (do **not** allow unauthenticated invocations when
prompted):

    gcloud functions deploy kcidb_load_message \
                            --runtime python37 \
                            --trigger-topic kernelci_new \
                            --env-vars-file main.env.yaml \
                            --retry \
                            --timeout=540

    gcloud functions deploy kcidb_spool_notifications \
                            --runtime python37 \
                            --trigger-topic kernelci_loaded \
                            --env-vars-file main.env.yaml \
                            --retry \
                            --timeout=540

    gcloud functions deploy kcidb_send_notification \
                            --runtime python37 \
                            --trigger-event providers/cloud.firestore/eventTypes/document.create \
                            --trigger-resource 'projects/kernelci-project/databases/(default)/documents/notifications/{notification_id}' \
                            --env-vars-file main.env.yaml \
                            --retry \
                            --timeout=540

NOTE: If you get a 403 Access Denied response to the first `gcloud functions
      deploy` invocation, try again. It might be a Google infrastructure quirk
      and could work the second time.

#### Grafana

See
[kcidb-grafana README.md](https://github.com/kernelci/kcidb-grafana/#setup)
for setup instructions.

#### CI System Accounts

Each submitting/querying CI system needs to have a service account created,
permissions assigned, and the account key generated. Below is an example for
a CI system called "CKI" having account named "kernelci-project-ci-cki" created.

Create the service account:

    gcloud iam service-accounts create kernelci-project-ci-cki

Grant the account query permissions for the BigQuery database:

    gcloud projects add-iam-policy-binding kernelci-project \
           --member "serviceAccount:kernelci-project-ci-cki@kernelci-project.iam.gserviceaccount.com" \
           --role "roles/bigquery.dataViewer"

    gcloud projects add-iam-policy-binding kernelci-project \
           --member "serviceAccount:kernelci-project-ci-cki@kernelci-project.iam.gserviceaccount.com" \
           --role "roles/bigquery.jobUser"

Grant the account permissions to submit to the `kernelci_new` Pub/Sub topic:

    gcloud pubsub topics add-iam-policy-binding kernelci_new \
                         --member="serviceAccount:kernelci-project-ci-cki@kernelci-project.iam.gserviceaccount.com" \
                         --role=roles/pubsub.publisher

Generate the account key file (`kernelci-project-ci-cki.json` here) for use by
the CI system:

    gcloud iam service-accounts keys create kernelci-project-ci-cki.json \
           --iam-account kernelci-project-ci-cki@kernelci-project.iam.gserviceaccount.com

### Upgrading

#### BigQuery

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
        kcidb-db-init -d kernelci02

3. Switch all data submitters to using new-schema kcidb and the newly-created
   dataset.

4. Create a new dataset with the name of the old one (`kernelci01` here), but
   with `_archive` suffix, using the old-schema kcidb:

        # Using old-schema kcidb
        kcidb-db-init -d kernelci01_archive

5. Using BigQuery management console, shedule copying the old dataset to the
   created dataset. When that is done, remove the old dataset.

6. Transfer data from the copy of the old dataset (named `kernelci01_archive`
   here) to the new dataset (named `kernelci02` here) using old-schema
   `kcidb-db-dump` and new-schema `kcidb-db-load`.

        # Using old-schema kcidb
        kcidb-db-dump -d kernelci01_archive > kernelci01_archive.json
        # Using new-schema kcidb
        kcidb-db-load -d kernelci02 < kernelci01_archive.json

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

To make a release tag the release commit with `v<NUMBER>`, where `<NUMBER>` is
the next release number, e.g. `v3`. The very next commit after the tag should
update the version number in `setup.py` to be the next one. I.e. continuing
the above example, it should be `4`.
