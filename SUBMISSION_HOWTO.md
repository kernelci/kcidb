How to submit reports with KCIDB
================================

Here's what you need to do.

1. Get submission credentials and parameters.
2. Install KCIDB.
3. Generate some report data.
4. Submit report data.
5. Go to 3, if you have more.

You don't need to run a daemon, just execute command-line tools (or use the
Python 3 library, if you're feeling fancy).

1\. Get submission credentials and parameters
---------------------------------------------

Write to [kernelci@groups.io](mailto:kernelci@groups.io), introduce yourself,
and explain what you want to submit (better, show preliminary report data).
Once your request is approved, you will get a JSON credentials file, which you
can use to authenticate yourself with KCIDB tools/library.

Export the file location (here `~/.kcidb-credentials.json`) into environment
like this:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/.kcidb-credentials.json
```

You will also need to specify some or all of the following to the
tools/library (using current production parameters from here on):

* Google Cloud project: `kernelci-production`
* Submission queue topic: `kernelci_new`
* Dataset name: `kernelci04`

Finally, we'll need to agree on the "origin" string identifying your system
among other submitters. We'll use `submitter` in examples below.

2\. Install KCIDB
-----------------

Pick a KCIDB [release][releases]. Latest would normally be best.

Let's say you picked v7. Run this:

```bash
pip3 install --user 'git+https://git@github.com/kernelci/kcidb.git@v7'
```

See [README.md](README.md) for alternatives, and if you know your Python, feel
free to do it your way!

To test your installation, authentication, and the parameters you received
execute an empty query on the database:

```console
$ kcidb-query -d kernelci04
{
    "builds": [],
    "revisions": [],
    "tests": [],
    "version": {
        "major": 3,
        "minor": 0
    }
}
```

and submit an empty report:

```console
$ echo '{"version":{"major":3,"minor":0}}' |
        kcidb-submit -p kernelci-production -t kernelci_new
```

Both should execute without errors and finish with zero exit status.

3\. Generate some report data
-----------------------------

KCIDB accepts data in JSON, described by a versioned schema. The
`kcidb-schema` tool will output the latest schema version. However, all tools
will accept data complying with older schema versions, as a rule. Any
exceptions to that will be mentioned in KCIDB release notes. Pipe your data
into `kcidb-validate` tool to check if it will be accepted.

Here's a minimal report, containing no data:

```json
{
    "version": {
        "major": 3,
        "minor": 0
    }
}
```

You can submit such a report, it will be accepted, but will have no effect on
the database or notifications.

### Objects

The schema describes three types of objects which can be submitted
independently or in any combination:
* "revision" - a version of the code being built and tested
* "build" - a build of a specific revision
* "test" - an execution of a test on a specific build in specific environment

Each of these object types refers to the previous one using an ID. The only
required fields for each object are their IDs, IDs of the parent object
(except for revisions), and the origin. Objects of each type are stored in
their own top-level array named respectively (in plural).

Here's an example of a report, containing only the required fields for a
revision with one build and one test:

```json
{
    "revisions": [
        {
            "id": "c9c9735c46f589b9877b7fc00c89ef1b61a31e18",
            "origin": "redhat"
        }
    ],
    "builds": [
        {
            "id": "redhat:956769",
            "revision_id": "c9c9735c46f589b9877b7fc00c89ef1b61a31e18",
            "origin": "redhat"
        }
    ],
    "tests": [
        {
            "id": "redhat:114353810",
            "build_id": "redhat:956769",
            "origin": "redhat"
        }
    ],
    "version": {
        "major": 3,
        "minor": 0
    }
}
```

If you're curious, you can query the complete objects above with this command:

    kcidb-query -d kernelci04 -t redhat:114353810 --parents

#### Revision IDs

Revision IDs are either complete git commit hashes, e.g.:

    db14560cba31b9fdf8454d097e5cb9e488c621fd

or a concatenation of a commit hash and a patchset hash with the plus `+`
character in between, e.g. (truncated):

    db14560cba31b9fdf8454d097e5cb9e488c621fd+769fef074514b330b...

The former is used to refer to committed revisions and the latter to patchsets
applied on top of certain commits.

A patchset hash can be generated using a shell command:

```console
$ sha256sum *.patch | cut -c-64 | sha256sum | cut -c-64
aa98bec5becf3edb58a18be346547c1a2a02e2bde02a06f9cc7e10dc35c6276c
```

Please make sure that whatever shell glob you use it expands into a list of
patches in order of application, or provide them explicitly.

In Python a patchset hash could be generated with a function like this one:

```python
import hashlib

def hash_patchset(mboxes):
    """
    Generate a patchset hash.

    Args:
        mboxes: A list of strings containing patchset mboxes,
                in order of application.

    Returns:
        A string containing the hexadecimal digest of the hash.
    """
    hash = hashlib.sha256()
    for mbox in mboxes:
        hash.update(
            (hashlib.sha256(mbox).hexdigest() + "\n").encode()
        )
    return hash.hexdigest()
```

#### Build and test IDs

Both build and test IDs have to start with your "origin" string, followed by
the colon `:` character, followed by your origin-local ID. The origin-local ID
can be any string, but must identify the object uniquely among all objects of
the same type you submit. E.g.:

    submitter:12
    submitter:db58a18be346
    submitter:test-394
    submitter:build-394

### Properties

Once you get the required properties (IDs and origins) generated, and have
your objects accepted by KCIDB, you can start adding the optional fields.
Some good starting candidates are described below. See the schema for more.

#### Revisions

##### `valid`
True if the revision is valid, i.e. if its parts could be combined. False if
not, e.g. if its patches failed to apply. Simply set to `True` if your
revision is a commit in a git repo, and you managed to check it out.

Example: `true`

##### `git_repository_url`
The URL of the Git repository which contains the base code of the revision.
The shortest possible `https://` URL, or, if that's not available, the
shortest possible `git://` URL.

Example: `"https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git"`

##### `git_commit_hash`
The full commit hash of the revision's base code. The same string you would
put into the `id` of an unpatched revision, or the start of the `id` of a
patched one.

Example: `"db14560cba31b9fdf8454d097e5cb9e488c621fd"`

##### `discovery_time`
The time the revision was discovered by the CI system. E.g. the time the CI
system found a patch message, or noticed a new commit or a new tag in a git
repo. As described by [RFC3339, 5.6 Internet Date/Time
Format][datetime_format].

Example: `"2020-08-14T23:08:06.967000+00:00"`

#### Builds

##### `valid`
True if the build is valid, i.e. if it succeeded. False if not.

Example: `true`

##### `architecture`
Target architecture of the build. Not standardized yet.

Example: `"x86_64"`

##### `compiler`
Name and version of the compiler used to make the build.

Example: `"gcc (GCC) 10.1.1 20200507 (Red Hat 10.1.1-1)"`

##### `start_time`
The time the build was started, according to [RFC3339, 5.6 Internet Date/Time
Format][datetime_format].

Example: `"2020-08-14T23:08:10.008000+00:00"`

#### Tests

##### `status`
The test status string, one of the following.

* `ERROR` - the test is faulty, the status of the tested code is unknown.
* `FAIL` - the test has failed, the tested code is faulty.
* `PASS` - the test has passed, the tested code is correct.
* `DONE` - the test has finished successfully, the status of the tested code
  is unknown.
* `SKIP` - the test wasn't executed, the status of the tested code is unknown.

The status names above are listed in priority order (highest to lowest), which
could be used for producing a summary status for a collection of test runs,
e.g. for all testing done on a build, based on results of executed test
suites. The summary status would be the highest priority status across all
test runs in a collection.

Example: `"FAIL"`

##### `waived`
True if the test status should be ignored.

Could be used for reporting test results without affecting the overall test
status and alerting the subscribers. For example, for collecting test
reliability statistics when the test is first introduced, or is being fixed.

Example: `false`

##### `path`
Dot-separated path to the node in the test classification tree the executed
test belongs to. The empty string signifies the root of the tree, i.e. all
tests for the build, executed by the origin CI system.

Please consult the [catalog of recognized tests][tests] for picking the
top-level name for your test, and submit a PR adding it if you don't find it.

Example: `"ltp.sem01"`

##### `start_time`
The time the test run was started, according to [RFC3339, 5.6 Internet
Date/Time Format][datetime_format].

Example: `"2020-08-14T23:41:54+00:00"`

### Extra data
If you have some data you'd like to provide developers with, but the schema
doesn't accommodate it, put it as arbitrary JSON under the `misc` field, which
is supported for every object. Then contact KCIDB developers with your
requirements.

For example, here's a `misc` field from KernelCI-native builds:

```json
{
    "build_platform" : [
        "Linux",
        "build-j7428-x86-64-gcc-8-x86-64-defconfig-wk45x",
        "4.15.0-1092-azure",
        "#102~16.04.1-Ubuntu SMP Tue Jul 14 20:28:23 UTC 2020",
        "x86_64",
        ""
    ],
    "kernel_image_size" : 9042816,
    "vmlinux_bss_size" : 1019904,
    "vmlinux_data_size" : 1595008
}
```

The kernel developers would already be able to see it in the dashboard, and
the KCIDB developers would have samples of your data which would help them
support it in the schema.

You can also put any debugging information you need into the `misc` fields.
E.g. you can add IDs of your internal objects corresponding to reported ones,
so you can track where they came from, like CKI does:

```json
{
    "beaker_recipe_id": 8687594,
    "beaker_task_id": 114353810,
    "job_id": 956777,
    "pipeline_id": 612127
}
```

4\. Submit report data
----------------------
As soon as you have your report data pass validation (e.g. with the
`kcidb-validate` tool), you should be able to submit it to the database.

If you're using shell, and e.g. have your data in file `report.json`, pipe it
to the `kcidb-submit` tool like this:

    kcidb-submit -p kernelci-production -t kernelci_new < report.json

If you're using Python 3, and e.g. have variable `report` holding standard
JSON representation of your report, you can submit it like this:

```python
import kcidb

client = kcidb.Client(project_id="kernelci-production",
                      topic_name="kernelci_new")
client.submit(report)
```

Your data could take up to a few minutes to reach the database, but after that
you should be able to find it in our [dashboard][dashboard], or query using
the `kcidb-query` command-line tool. For example, if you want to retrieve a
revision object you submitted with ID
`c9c9735c46f589b9877b7fc00c89ef1b61a31e18`, execute:

    kcidb-query -d kernelci04 -r c9c9735c46f589b9877b7fc00c89ef1b61a31e18

If you want to retrieve all its builds and tests as well, execute:

    kcidb-query -d kernelci04 -r c9c9735c46f589b9877b7fc00c89ef1b61a31e18 -c

See the output of `kcidb-query --help` for usage information, including how to
retrieve builds, tests, and how to retrieve object parents.

### Submitting directly

If for any reason you cannot use the command-line tools, and you don't use
Python 3 (e.g. you are using another language in a "serverless" environment),
you can implement interfacing with KCIDB submission system directly.

NOTE: this interface is less stable than the command-line, and the library
interfaces, and is more likely to change in the future.

You will have to use one of the Google Cloud [Pub/Sub client
libraries][pub_sub_libraries] or [service APIs][pub_sub_apis] to publish your
reports to the Pub/Sub topic specified above, using the provided credentials.
Please make sure to validate each report against the schema output by
`kcidb-schema` before publishing it.

5\. Go to 3, if you have more
------------------------------
Do not hesitate to start submitting your reports as soon as you can. This will
let you, kernel developers, and KCIDB developers see how it works, what
changes/additions might be needed to both your data and KCIDB, and improve our
reporting faster!

[releases]: https://github.com/kernelci/kcidb/releases
[datetime_format]: https://tools.ietf.org/html/rfc3339#section-5.6
[tests]: https://github.com/kernelci/kcidb/blob/master/tests.yaml
[dashboard]: https://staging.kernelci.org:3000/
[pub_sub_libraries]: https://cloud.google.com/pubsub/docs/reference/libraries
[pub_sub_apis]: https://cloud.google.com/pubsub/docs/reference/service_apis_overview
