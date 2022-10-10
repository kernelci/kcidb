"""
Kernel CI report object-oriented (OO) data representation.
"""

import sys
from abc import ABC, abstractmethod
from functools import reduce
from cached_property import cached_property
import kcidb.db
from kcidb.misc import LIGHT_ASSERTS
from kcidb.orm import Type, SCHEMA, Pattern, Source


class Object:
    """An object-oriented representation of a database object"""

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

    def get_type(self):
        """
        Retrieve the object's type.

        Returns:
            The object's type, an instance of kcidb.orm.Type.
        """
        return self._type

    def get_id(self):
        """
        Retrieve a tuple of field values identifying the object globally.

        Returns:
            A tuple of values of object fields identifying it globally.
        """
        return self._type.get_id(self._data)

    def get_parent_id(self, parent_type_name):
        """
        Retrieve a tuple of field values identifying the object's parent of
        particular type globally.

        Args:
            parent_type_name:   The name of the type of the parent object to
                                retrieve the ID of.

        Returns:
            A tuple of values of the object fields identifying the parent
            globally.
        """
        return self._type.get_parent_id(parent_type_name, self._data)

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
    "FAIL":     1,
    "ERROR":    2,
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

# A dictionary of test "waived" values and dictionaries of test "status"
# values and their combined priorities, defined as a positive integer, with
# lower values meaning higher priority. Sorted higher priority first.
TEST_WAIVED_STATUS_PRIORITY = {
    waived: {
        status:
        (waived_priority - 1) * len(TEST_STATUS_PRIORITY) + status_priority
        for status, status_priority in TEST_STATUS_PRIORITY.items()
    }
    for waived, waived_priority in TEST_WAIVED_PRIORITY.items()
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


class BuildTestContainer(BuildContainer, TestContainer):
    """Abstract build container, exposing linked tests"""

    @cached_property
    def tests(self):
        """A list of tests for this container"""
        return reduce(lambda x, y: x + y,
                      (build.tests for build in self.builds),
                      [])


class BugContainer(ABC):
    """Abstract bug container"""
    @abstractmethod
    def bugs(self):
        """A list of reports for the issues incident to this container"""

    @property
    def code_bugs(self):
        """A list of reports for code issues incident to this container"""
        return [bug for bug in self.bugs if bug.culprit_code]

    @property
    def tool_bugs(self):
        """A list of reports for tool issues incident to this container"""
        return [bug for bug in self.bugs if bug.culprit_tool]

    @property
    def harness_bugs(self):
        """A list of reports for harness issues incident to this container"""
        return [bug for bug in self.bugs if bug.culprit_harness]


class IssueContainer(ABC):
    """Abstract issue container"""
    @abstractmethod
    def issues(self):
        """A list of issues incident to this container"""

    @property
    def code_issues(self):
        """A list of code issues incident to this container"""
        return [issue for issue in self.issues if issue.culprit_code]

    @property
    def tool_issues(self):
        """A list of tool issues incident to this container"""
        return [issue for issue in self.issues if issue.culprit_tool]

    @property
    def harness_issues(self):
        """A list of harness issues incident to this container"""
        return [issue for issue in self.issues if issue.culprit_harness]


class IncidentContainer(ABC):
    """Abstract incident container"""
    @abstractmethod
    def incidents(self):
        """A list of incidents of this container"""


class IncidentIssueBugContainer(IncidentContainer, IssueContainer,
                                BugContainer):
    """Abstract incident container, exposing linked issues and bugs"""

    @cached_property
    def issues(self):
        """A list of issues incident to this container"""
        return list({
            incident.issue for incident in self.incidents if incident.issue
        })

    @cached_property
    def bugs(self):
        """A list of reports for the issues incident to this container"""
        return list({
            issue.bug for issue in self.issues if issue.bug
        })


class Node(IncidentIssueBugContainer):
    """A test node"""

    def __init__(self, parent, name):
        """
        Initialize a test node.

        Args:
            parent: The parent object of this test node - another Node or a
                    TestContainer.
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
    def incidents(self):
        """A list of incidents for this and all child nodes"""
        return reduce(lambda x, y: x + y,
                      (test.incidents for test in self.tests),
                      [])

    @property
    def waived(self):
        """The summarized waived value of this test node"""
        return self.waived_status[0]

    @property
    def status(self):
        """The summarized status value of this test node"""
        return self.waived_status[1]

    @cached_property
    def waived_status(self):
        """The summarized waived and status values of this test node"""
        return min(
            ((test.waived, test.status) for test in self.tests),
            key=lambda ws: TEST_WAIVED_STATUS_PRIORITY[ws[0]][ws[1]],
            default=(None, None)
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


# We'll fix it, pylint: disable=too-many-ancestors
class Revision(Object, BuildTestContainer, IncidentIssueBugContainer):
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
        # pylint: disable=modified-iterating-dict
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
    def incidents(self):
        """A list of incidents of this revisions's builds and tests"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return reduce(lambda x, y: x + y,
                      (checkout.incidents for checkout in self.checkouts),
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


# We'll fix it, pylint: disable=too-many-ancestors
class Checkout(Object, BuildTestContainer, IncidentIssueBugContainer):
    """An OO-representation of a checkout"""

    # Force ABC to recognize abstract method definition
    @cached_property
    def builds(self):
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return self.__getattr__("builds")

    @cached_property
    def incidents(self):
        """A list of incidents of this checkout's builds and tests"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return reduce(lambda x, y: x + y,
                      (build.incidents for build in self.builds),
                      [])

    @property
    def git_repository_url_branch(self):
        """A tuple containing both the git repository URL and the branch"""
        return self.git_repository_url, self.git_repository_branch


# We'll fix it, pylint: disable=too-many-ancestors
class Build(Object, TestContainer,
            IncidentContainer, IssueContainer, BugContainer):
    """An OO-representation of a build"""

    # Force ABC to recognize abstract method definition
    @cached_property
    def tests(self):
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return self.__getattr__("tests")

    @cached_property
    def incidents(self):
        """A list of incidents of this build and its tests"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return self.build_incidents + self.test_incidents

    @cached_property
    def issues(self):
        """A list of issues incident to this build and its tests"""
        return list(set(self.build_issues) | set(self.test_issues))

    @cached_property
    def bugs(self):
        """
        A list of reports for the issues incident to this build and its tests.
        """
        return list(set(self.build_bugs) | set(self.test_bugs))

    @cached_property
    def build_incidents(self):
        """A list of build incidents of this build"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return self.__getattr__("incidents")

    @cached_property
    def build_issues(self):
        """A list of issues incident to this build"""
        return list({i.issue for i in self.build_incidents if i.issue})

    @cached_property
    def build_bugs(self):
        """A list of reports for the issues incident to this build"""
        return list({i.bug for i in self.build_issues if i.bug})

    @cached_property
    def test_incidents(self):
        """A list of incidents of this build's tests"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return reduce(lambda x, y: x + y,
                      (test.incidents for test in self.tests),
                      [])

    @cached_property
    def test_issues(self):
        """A list of issues incident to build's tests"""
        return list({i.issue for i in self.test_incidents if i.issue})

    @cached_property
    def test_bugs(self):
        """A list of reports for the issues incident to build's tests"""
        return list({i.bug for i in self.test_issues if i.bug})

    @cached_property
    def valid(self):
        """The "valid" value with incidents accounted"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        valid = min(
            (issue.build_valid for issue in self.build_issues),
            key=lambda v: VALID_PRIORITY[v], default=None
        )
        return self.__getattr__("valid") if valid is None else valid


class Test(Object, IncidentIssueBugContainer):
    """An OO-representation of a test"""

    # prevent class from being collected by unittest.
    __test__ = False

    # Force ABC to recognize abstract method definition
    @cached_property
    def incidents(self):
        """A list of incidents of this container"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return self.__getattr__("incidents")

    @cached_property
    def status(self):
        """The "status" value with incidents accounted"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        status = min(
            (issue.test_status for issue in self.issues),
            key=lambda s: TEST_STATUS_PRIORITY[s], default=None
        )
        return self.__getattr__("status") if status is None else status


class Bug(Object, IssueContainer, IncidentContainer):
    """An OO-representation of an issue report"""

    # Force ABC to recognize abstract method definition
    @cached_property
    def issues(self):
        """A list of issues incident to this container"""
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return self.__getattr__("issues")

    @cached_property
    def incidents(self):
        """A list of incidents of this container"""
        return reduce(lambda x, y: x + y,
                      (issue.incidents for issue in self.issues),
                      [])

    @cached_property
    def builds(self):
        """A list of builds with this bug"""
        return list(reduce(lambda x, y: x | y,
                    (set(issue.builds) for issue in self.issues),
                    set()))

    @cached_property
    def tests(self):
        """A list of test runs with this bug"""
        return list(reduce(lambda x, y: x | y,
                    (set(issue.tests) for issue in self.issues),
                    set()))

    @cached_property
    def checkouts(self):
        """A list of checkouts with this bug"""
        return list(reduce(lambda x, y: x | y,
                    (set(issue.checkouts) for issue in self.issues),
                    set()))

    @cached_property
    def revisions(self):
        """A list of revisions with this bug"""
        return list(reduce(lambda x, y: x | y,
                    (set(issue.revisions) for issue in self.issues),
                    set()))


class Issue(Object, IncidentContainer, BuildContainer, TestContainer):
    """An OO-representation of an issue"""

    # Force ABC to recognize abstract method definition
    @cached_property
    def incidents(self):
        # It isn't, pylint: disable=bad-option-value,unnecessary-dunder-call
        return self.__getattr__("incidents")

    @cached_property
    def builds(self):
        """A list of builds with this issue"""
        return list({
            incident.build for incident in self.incidents if incident.build
        })

    @cached_property
    def tests(self):
        """A list of test runs with this issue"""
        return list({
            incident.test for incident in self.incidents if incident.test
        })

    @cached_property
    def checkouts(self):
        """A list of checkouts with this issue"""
        return list(
            {build.checkout for build in self.builds
             if build.checkout} |
            {test.build.checkout for test in self.tests
             if test.build and test.build.checkout}
        )

    @cached_property
    def revisions(self):
        """A list of revisions with this issue"""
        return list({
            checkout.revision for checkout in self.checkouts
            if checkout.revision
        })


class Incident(Object):
    """An OO-representation of an incident"""


# A map of object type names and Object-derived classes handling their data
CLASSES = dict(
    revision=Revision,
    checkout=Checkout,
    build=Build,
    test=Test,
    bug=Bug,
    issue=Issue,
    incident=Incident,
)

assert set(CLASSES) == set(SCHEMA.types)


class Client:
    """Object-oriented data client"""

    def __init__(self, source, prefetch=True, cache=True, sort=False):
        """
        Initialize the client.

        Args:
            source:     Raw object-oriented data source, an instance of
                        kcidb.orm.Source.
            prefetch:   If True, prefetch data using kcidb.orm.Prefetcher.
                        If False, do not prefetch. Doesn't really make much
                        sense without caching enabled too.
            cache:      If True, cache the retrieved data using
                        kcidb.orm.Cache. If False, do not cache.
            sort:       If True, sort data fetched from the source (useful for
                        tests). If False, do not sort.
        """
        assert isinstance(source, Source)
        assert isinstance(sort, bool)
        self.source = source
        self.cache = None
        if cache:
            self.cache = kcidb.orm.Cache(self.source)
            self.source = self.cache
        if prefetch:
            self.source = kcidb.orm.Prefetcher(self.source)
        self.sort = sort

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
        data = {
            obj_type_name: [
                CLASSES[obj_type_name](
                    self, SCHEMA.types[obj_type_name], obj_data
                )
                for obj_data in obj_data_list
            ]
            for obj_type_name, obj_data_list in
            self.source.oo_query(pattern_set).items()
        }
        if self.sort:
            data = {
                type_name: sorted(objs, key=lambda obj: obj.get_id())
                for type_name, objs in
                sorted(data.items(), key=lambda item: item[0])
            }
        return data

    def reset_cache(self):
        """
        Reset the cache, if enabled. No effect, if the cache was disabled.
        """
        if self.cache:
            self.cache.reset()


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
