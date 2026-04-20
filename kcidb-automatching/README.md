
# KCIDB Match Tool

## Overview

The KCIDB Match Tool is designed to manage and validate match patterns objects for issues in a KCIDB (Kernel CI Database) schema and generate incidents from kcidb_io objects when they match stored patterns.
This tool is intended to be integrated with KCIDB to automatically create incidents based on existing issues with defined match patterns.
This tool can add pattern objects from issues to an SQLite database and validate input objects against these pattern objects. The tool also provides functionalities to add, update, delete pattern objects, and generate incidents.

## Schema Explanation

### Pattern Object

The pattern object is extracted from a field called `pattern_object` under the `misc` field of an issue object.
The pattern object follows the KCIDB-IO schema but without the required fields. This means the pattern object can have incomplete data as long as the data matches the structure of the KCIDB-IO schema.

The `--update-patterns` flag processes all issues, while running `./kcidb_match.py` without parameters assumes a maximum of 3 elements (one for checkout, one for build, and one for test) with parental conditions. If there are more elements in each list, only the first will be considered.

### SQLite Table

The SQLite table used to store pattern objects has the following structure:

```sql
CREATE TABLE IF NOT EXISTS patterns (
    issue_id TEXT UNIQUE,
    issue_version INTEGER,
    pattern_object JSON
);
```

- `issue_id`: The unique identifier for the issue.
- `issue_version`: The version of the issue.
- `pattern_object`: The pattern object of the issue in JSON format.


## Usage

### Adding Patterns

To add pattern objects to the database, use the `--update-patterns` flag. The input should be a JSON file with issues.

#### Example `issues.json`

```json
{
    "version": {
        "major": 4,
        "minor": 3
    },
    "issues": [
        {
            "id": "kernelci_api:70d17807303641a9d6d2a8aeb1aee829221cefcf",
            "version": 0,
            "origin": "kernelci_api",
            "report_url": "https://linux-regtracking.leemhuis.info/regzbot/regression/lore/20240607151439.175035-1-laura.nao@collabora.com/",
            "report_subject": "RIP: 0010:usercopy_abort+0x74/0x76 kernel panic",
            "culprit": {
                "code": true,
                "tool": false,
                "harness": false
            },
            "comment": "",
            "misc": {
                "author": {
                    "name": "Laura Nao",
                    "email": "laura.nao@collabora.com"
                },
                "pattern_object": {
                    "checkouts": [
                        {
                            "tree_name": "next"
                        }
                    ],
                    "builds": [
                        {
                            "compiler": "gcc.*"
                        }
                    ]
                }
            }
        }
    ]
}
```

#### Adding Patterns Command

```sh
cat issues.json | ./kcidb_match.py --update-patterns
```

#### Removing Patterns

To delete a pattern entry from the database, submit the issue without the `pattern_object` under `misc`.

### Checking Matches

To check an input object against the stored patterns objects, simply run the script without any flags. The input should be a JSON file following the KCIDB schema with a node and its parent nodes.

#### Example `sample_single.json`

```json
{
    "checkouts": [
        {
            "id": "ci-system:unique-checkout-id",
            "origin": "ci-system",
            "tree_name": "next"
        }
    ],
    "builds": [
        {
            "checkout_id": "ci-system:unique-checkout-id",
            "id": "ci-system:unique-build-id",
            "origin": "ci-system",
            "compiler": "gcc-10"
        }
    ]
}
```

#### Checking Matches Command

```sh
cat sample_single.json | ./kcidb_match.py
```

Output:

```sh
Matched issue ID: kernelci_api:70d17807303641a9d6d2a8aeb1aee829221cefcf Version: 0
```

### Generating Incidents

To generate incidents when a match is found, use the `--generate-incidents` flag. This will create an object following KCIDB schema with incidents objects.

#### Generating Incidents Command

```sh
cat sample_single.json | ./kcidb_match.py --generate-incidents
```

### Query Examples

#### Update Patterns Example

```sh
kcidb-query -i "kernelci_api:70d17807303641a9d6d2a8aeb1aee829221cefcf" -d "postgresql:host=127.0.0.1 port=5432 sslmode=disable dbname=playground_kcidb user=helen.koike@collabora.com" | ./kcidb-match.py --update-patterns
```

#### Checking Matches Example

```sh
kcidb-query -t "maestro:6690dbfc7488a1b744200e82" -d "postgresql:host=127.0.0.1 port=5432 sslmode=disable dbname=playground_kcidb user=helen.koike@collabora.com" --parents | ./kcidb-match.py
```

Output:

```sh
Matched issue ID: kernelci_api:70d17807303641a9d6d2a8aeb1aee829221cefcf Version: 0
```

### Generating Incidents with --ignore-db

The --ignore-db argument can be used to generate incidents based solely on the issues field in the KCIDB-IO object provided via stdin, ignoring the database. This is useful when you want to test patterns before saving them to the database.


#### Example `sample_single_with_issue.json`

```json
{
    "checkouts": [
        {
            "id": "ci-system:unique-checkout-id",
            "origin": "ci-system",
            "tree_name": "next"
        }
    ],
    "builds": [
        {
            "checkout_id": "ci-system:unique-checkout-id",
            "id": "ci-system:unique-build-id",
            "origin": "ci-system",
            "compiler": "gcc-10"
        }
    ],
    "issues": [
    {
        "misc": {
            "pattern_object": {
                "checkouts": [
                    {
                        "tree_name": "next"
                    }
                ],
                "builds": [
                    {
                        "compiler": "gcc.*"
                    }
                ]
            }
        }
    }
}
```

Example command:

```sh
cat sample_single_with_issue | ./kcidb_match.py --ignore-db --generate-incidents
```
### Check Test ID or Build ID

The --check_test_id and --check_build_id arguments allow you to check a specific test or build ID against the provided issues in stdin, implying --ignore-db. It fetches those nodes from KCIDB using kcidb-query. These options require a database connection string provided with -d.

Example commands:

```sh
cat issue.json | ./kcidb-match.py --check_test_id "maestro:6690dbfc7488a1b744200e82" -d "postgresql:host=127.0.0.1 port=5432 sslmode=disable dbname=playground_kcidb user=helen.koike@collabora.com"
```

```sh
cat issue.json | ./kcidb-match.py --check_test_id "maestro:6690dbfc7488a1b744200e82" -d "postgresql:host=127.0.0.1 port=5432 sslmode=disable dbname=playground_kcidb user=helen.koike@collabora.com"
```

These commands fetch the KCIDB-IO object corresponding to the provided test or build ID, check it against the issues provided via stdin, and generate incidents accordingly.