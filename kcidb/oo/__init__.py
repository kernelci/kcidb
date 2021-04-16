"""
Kernel CI report object-oriented (OO) data representation.
"""

import re
import sys
from abc import ABC, abstractmethod
from functools import reduce
from cached_property import cached_property
import kcidb.db
from kcidb.misc import LIGHT_ASSERTS
from kcidb.orm import Type, SCHEMA, Pattern, Source


class Object:
    """An object-oriented representation of a database object"""

    # A regular expression matching valid object summaries
    SUMMARY_RE = re.compile(r"[^\x00-\x1f\x7f]*")

    # Calm down, pylint: disable=invalid-name
    def __init__(self, client, type, data):
        """
        Initialize the representation.

        Args:
            client:     The object-oriented database client to query for
                        references.
            type:       The type of represented object.
                        Instance of kcidb.orm.Type.
            data:       The raw data of the object to represent.
        """
        assert isinstance(client, Client)
        assert isinstance(type, Type)
        assert LIGHT_ASSERTS or type.is_valid(data)
        self._client = client
        self._type = type
        self._data = data

    def get_id(self):
        """
        Retrieve a tuple of field values identifying the object globally.

        Returns:
            A tuple of values of object fields identifying it globally.
        """
        return self._type.get_id(self._data)

    def __hash__(self):
        return hash(self.get_id())

    def __eq__(self, other):
        return isinstance(other, Object) and self.get_id() == other.get_id()

    def __getattr__(self, name):
        if name in self._data:
            return self._data[name]
        id = self.get_id()
        if name in self._type.parents:
            response = self._client.query(
                Pattern.parse(
                    ">" + self._type.name + "%<" + name + "#", [{id}]
                )
            )
            try:
                return response[name][0]
            except (KeyError, IndexError):
                return None
        if name.endswith("s"):
            child_type_name = name[:-1]
            if child_type_name in self._type.children:
                return self._client.query(
                    Pattern.parse(
                        ">" + self._type.name + "%>" + child_type_name + "#",
                        [{id}]
                    )
                )[child_type_name]
        raise AttributeError(f"Attribute {name!r} not found")

    def summarize(self):
        """
        Generate a text summary of the data node.

        Returns:
            A single-line string summary of the data node.
        """
        summary = self._type.summary_template.render({
            self._type.name: self,
        })
        assert Object.SUMMARY_RE.fullmatch(summary), \
            f"Summary is invalid: {repr(summary)}"
        return summary

    def describe(self):
        """
        Generate a detailed text description of the object.

        Returns:
            A (multiline) string describing the object in detail.
        """
        return self._type.description_template.render({
            self._type.name: self,
        })


# A dictionary of "valid" values and their priority, defined as a positive
# integer, with lower values meaning higher priority. Sorted higher priority
# first.
VALID_PRIORITY = {
    False:  1,
    True:   2,
    None:   3,
}

# A dictionary of test "status" values and their priority, defined as a
# positive integer, with lower values meaning higher priority. Sorted higher
# priority first.
TEST_STATUS_PRIORITY = {
    "ERROR":    1,
    "FAIL":     2,
    "PASS":     3,
    "DONE":     4,
    "SKIP":     5,
    None:       6,
}

# A dictionary of test "waived" values and their priority, defined as a
# positive integer, with lower values meaning higher priority. Sorted higher
# priority first.
TEST_WAIVED_PRIORITY = {
    False:  1,
    True:   2,
    None:   3,
}


class BuildContainer(ABC):
    """Abstract build container"""
    @abstractmethod
    def builds(self):
        """A list of builds in this container"""

    @cached_property
    def builds_valid(self):
        """
        Status of this container's builds.

        True if all passed, false if at least one failed, and None, if there
        were no builds, or their status isn't known.
        """
        return min(
            (build.valid for build in self.builds),
            key=lambda valid: VALID_PRIORITY[valid],
            default=None
        )

    @cached_property
    def architecture_valid_builds(self):
        """
        A dictionary of names of architectures and dictionaries of build
        "valid" values and corresponding builds, sorted from more to least
        important.
        """
        architecture_valid_builds = {}
        for build in self.builds:
            if build.architecture not in architecture_valid_builds:
                architecture_valid_builds[build.architecture] = {
                    valid: [] for valid in VALID_PRIORITY
                }
            valid_builds = architecture_valid_builds[build.architecture]
            valid_builds[build.valid].append(build)
        return dict(sorted(
            architecture_valid_builds.items(),
            key=lambda item: tuple(
                len(item[1][valid]) for valid in VALID_PRIORITY
            ),
            reverse=True
        ))


class TestContainer(ABC):
    """Abstract test container"""
    @abstractmethod
    def tests(self):
        """A list of tests in this container"""

    @cached_property
    def tests_root(self):
        """The root test node"""
        return Node(self, "")


class Node:
    """A test node"""

    def __init__(self, parent, name):
        """
        Initialize a test node.

        Args:
            parent: The parent object of this test node a test container.
            name:   The name of this node in parent's scope, or None for the
                    node corresponding to tests without paths.
        """
        assert isinstance(parent, (TestContainer, Node))
        assert isinstance(name, (str, type(None)))
        self.parent = parent
        self.name = name

    @cached_property
    def path(self):
        """The test node path"""
        if self.name is not None and \
           isinstance(self.parent, Node) and \
           self.parent.path != "":
            return self.parent.path + "." + self.name
        return self.name

    @cached_property
    def tests(self):
        """A list of tests (test runs) for this and all child nodes"""
        if self.path == "":
            return self.parent.tests
        return [
            test for test in self.parent.tests
            if test.path == self.path or (
                self.path is not None and
                test.path is not None and
                test.path.startswith(self.path + ".")
            )
        ]

    @cached_property
    def status(self):
        """The summarized status of this test node"""
        return min(
            (test.status for test in self.tests),
            key=lambda status: TEST_STATUS_PRIORITY[status],
            default=None
        )

    @cached_property
    def waived(self):
        """The summarized waiving status of this test node"""
        return min(
            (test.waived for test in self.tests),
            key=lambda waived: TEST_WAIVED_PRIORITY[waived],
            default=None
        )

    @cached_property
    def nodes(self):
        """A dictionary of child test node names and objects."""
        if self.path is None:
            return {}
        nodes = {}
        prefix = "" if self.path == "" else (self.path + ".")
        # For all test runs of this node or children nodes
        for test in self.tests:
            # If this is a test run without a path
            if test.path is None:
                name = None
            # Else, if this is a test for a child node
            elif len(test.path) > len(prefix):
                subpath = test.path[len(prefix):]
                try:
                    name = subpath[:subpath.index('.')]
                except ValueError:
                    name = subpath
            else:
                continue
            # Add node if not added yet
            if name not in nodes:
                nodes[name] = Node(self, name)
        return nodes

    @cached_property
    def waived_status_nodes(self):
        """
        A dictionary of all waived values and dictionaries of all status
        values and lists of nodes with corresponding waived and status values,
        all in order of decreasing priority.
        """
        waived_status_nodes = {
            waived: {status: [] for status in TEST_STATUS_PRIORITY}
            for waived in TEST_WAIVED_PRIORITY
        }
        for node in self.nodes.values():
            waived_status_nodes[node.waived][node.status].append(node)
        return waived_status_nodes

    def __getitem__(self, name):
        assert isinstance(name, (str, type(None)))
        return Node(self, name)

    def __iter__(self):
        names = set()
        prefix = "" if self.path == "" else (self.path + ".")
        # For all test runs of this node or children nodes
        for test in self.tests:
            # If this is a test run without a path
            if test.path is None:
                name = None
            # Else, if this is a test for a child node
            elif len(test.path) > len(prefix):
                subpath = test.path[len(prefix):]
                try:
                    name = subpath[:subpath.index('.')]
                except ValueError:
                    name = subpath
            else:
                continue
            if name not in names:
                names.add(name)
                yield name


class Revision(Object, BuildContainer, TestContainer):
    """An OO-representation of a revision"""

    @cached_property
    def repo_branch_checkouts(self):
        """
        A dictionary of repository URLs and dictionaries of branch names and
        lists of their checkouts, all sorted by the number of checkouts.
        """
        repo_branch_checkouts = {}
        # Collect repos, branches and checkouts
        for checkout in self.checkouts:
            repo = checkout.git_repository_url
            branch = checkout.git_repository_branch
            if repo not in repo_branch_checkouts:
                repo_branch_checkouts[repo] = {}
            branch_checkouts = repo_branch_checkouts[repo]
            if branch not in branch_checkouts:
                branch_checkouts[branch] = []
            branch_checkouts[branch].append(checkout)
        # Sort branches by number of checkouts
        for repo in repo_branch_checkouts:
            repo_branch_checkouts[repo] = dict(sorted(
                repo_branch_checkouts[repo].items(),
                key=lambda branch_checkouts: len(branch_checkouts[1]),
                reverse=True
            ))
        # Sort repos by number of checkouts
        repo_branch_checkouts = dict(sorted(
            repo_branch_checkouts.items(),
            key=lambda item: sum(
                len(checkouts) for checkouts in item[1].values()
            ),
            reverse=True
        ))
        return repo_branch_checkouts

    @cached_property
    def builds(self):
        """A list of builds of this revision"""
        return reduce(lambda x, y: x + y,
                      (checkout.builds for checkout in self.checkouts),
                      [])

    @cached_property
    def tests(self):
        """A list of tests for this revision"""
        return reduce(lambda x, y: x + y,
                      (checkout.tests for checkout in self.checkouts),
                      [])

    @cached_property
    def checkouts_valid(self):
        """
        Status of this revision's checkouts.

        True if all passed, false if at least one failed, and None, if there
        were no checkouts, or their status isn't known.
        """
        return min(
            (checkout.valid for checkout in self.checkouts),
            key=lambda valid: VALID_PRIORITY[valid],
            default=None
        )


class Checkout(Object, BuildContainer, TestContainer):
    """An OO-representation of a checkout"""

    # Force ABC to recognize abstract method definition
    @cached_property
    def builds(self):
        return self.__getattr__("builds")

    @cached_property
    def tests(self):
        """A list of tests for this checkout"""
        return reduce(lambda x, y: x + y,
                      (build.tests for build in self.builds),
                      [])


class Build(Object, TestContainer):
    """An OO-representation of a build"""

    # Force ABC to recognize abstract method definition
    @cached_property
    def tests(self):
        return self.__getattr__("tests")


class Test(Object):
    """An OO-representation of a test"""


# A map of object type names and Object-derived classes handling their data
CLASSES = dict(
    revision=Revision,
    checkout=Checkout,
    build=Build,
    test=Test,
)

assert set(CLASSES) == set(SCHEMA.types)


class Client:
    """Object-oriented data client"""

    def __init__(self, source):
        """
        Initialize the client.

        Args:
            source: Raw object-oriented data source, an instance of
                    kcidb.orm.Source.
        """
        assert isinstance(source, Source)
        self.source = source

    def query(self, pattern_set):
        """
        Retrieve objects specified via a pattern list.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, Pattern) for r in pattern_set)
        return {
            obj_type_name: [
                CLASSES[obj_type_name](
                    self, SCHEMA.types[obj_type_name], obj_data
                )
                for obj_data in obj_data_list
            ]
            for obj_type_name, obj_data_list in
            self.source.oo_query(pattern_set).items()
        }


class ArgumentParser(kcidb.misc.ArgumentParser):
    """
    Command-line argument parser with common OO arguments added.
    """

    def __init__(self, *args, database=None, **kwargs):
        """
        Initialize the parser, adding common OO arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            database:   The default database specification to use, or None to
                        make database specification required.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        kcidb.db.argparse_add_args(self, database=database)
        kcidb.orm.argparse_add_args(self)


class OutputArgumentParser(kcidb.misc.OutputArgumentParser):
    """
    Command-line argument parser for tools outputting JSON,
    with common OO arguments added.
    """

    def __init__(self, *args, database=None, **kwargs):
        """
        Initialize the parser, adding JSON output arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            database:   The default database specification to use, or None to
                        make database specification required.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        kcidb.db.argparse_add_args(self, database=database)
        kcidb.orm.argparse_add_args(self)


def query_main():
    """Execute the kcidb-oo-query command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = \
        "kcidb-oo-query - Query object-oriented data from " \
        "Kernel CI report database"
    parser = OutputArgumentParser(description=description)
    args = parser.parse_args()
    db_client = kcidb.db.Client(args.database)
    pattern_set = set()
    for pattern_string in args.pattern_strings:
        pattern_set |= kcidb.orm.Pattern.parse(pattern_string)
    kcidb.misc.json_dump(
        db_client.oo_query(pattern_set),
        sys.stdout, indent=args.indent, seq=args.seq
    )
