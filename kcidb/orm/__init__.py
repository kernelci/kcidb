"""
Kernel CI report object-relational mapping (ORM) - report data organized into
objects, but without the object-oriented interface.
"""

import logging
from abc import ABC, abstractmethod
from kcidb.misc import LIGHT_ASSERTS
from kcidb.orm import query, data

# Module's logger
LOGGER = logging.getLogger(__name__)


class Source(ABC):
    """An abstract source of raw object-oriented (OO) data"""

    @abstractmethod
    def oo_query(self, pattern_set):
        """
        Retrieve raw data for objects specified via a pattern set.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.query.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            raw data of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, query.Pattern) for r in pattern_set)


class Prefetcher(Source):
    """A prefetching source of object-oriented data"""

    def __init__(self, source):
        """
        Initialize the prefetching source.

        Args:
            source: The source to request objects from.
        """
        assert isinstance(source, Source)
        self.source = source

    def oo_query(self, pattern_set):
        """
        Retrieve raw data for objects specified via a pattern set.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.query.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            raw data of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, query.Pattern) for r in pattern_set)
        # First fetch the response we were asked for
        response = self.source.oo_query(pattern_set)
        # Generate patterns for all children of fetched root objects
        prefetch_pattern_set = set()
        for obj_type_name, objs in response.items():
            obj_type = data.SCHEMA.types[obj_type_name]
            if not obj_type.parents and objs:
                # TODO Get rid of formatting and parsing
                # It isn't,
                # pylint: disable=bad-option-value,unnecessary-dunder-call
                prefetch_pattern_set |= query.Pattern.parse(
                    query.Pattern(None, True, obj_type,
                                  {obj_type.get_id(obj) for obj in objs}).
                    __repr__(final=False) + ">*#"
                )
        # Prefetch, if generated any patterns
        if prefetch_pattern_set:
            LOGGER.info("Prefetching %r", prefetch_pattern_set)
            self.source.oo_query(prefetch_pattern_set)

        # Return the response for the original request
        return response


class Cache(Source):
    """A cache source of object-oriented data"""

    def __init__(self, source):
        """
        Initialize the cache source.

        Args:
            source: The source to request uncached objects from.
        """
        assert isinstance(source, Source)
        self.source = source
        self.reset()

    def reset(self):
        """
        Reset the cache.
        """
        self.type_id_objs = {type_name: {} for type_name in data.SCHEMA.types}
        self.pattern_responses = {}

    def _merge_pattern_response(self, pattern, response):
        """
        Process a response to a single-pattern query fetched from the
        underlying source, merging it with the cache.

        Args:
            pattern:    The pattern the response was retrieved for.
            response:   The retrieved response. May be modified.

        Returns:
            The merged response.
        """
        # Let's get it working first, refactor later,
        # pylint: disable=too-many-locals,too-many-branches
        assert isinstance(pattern, query.Pattern)
        assert LIGHT_ASSERTS or data.SCHEMA.is_valid(response)
        assert len(response) <= 1
        if not response:
            response[pattern.obj_type.name] = []
        type_name, objs = tuple(response.items())[0]
        assert type_name == pattern.obj_type.name

        # Merge the response and the cache
        get_id = data.SCHEMA.types[type_name].get_id
        get_parent_id = data.SCHEMA.types[type_name].get_parent_id
        id_objs = self.type_id_objs[type_name]
        base_pattern = pattern.base
        base_type = None if base_pattern is None else base_pattern.obj_type
        cached = set()
        # For each object in the response
        for obj in objs:
            # Deduplicate or cache the object
            # We like our "id", pylint: disable=invalid-name
            id = get_id(obj)
            if id in id_objs:
                obj = id_objs[id]
                LOGGER.debug("Deduplicated %r %r", type_name, id)
            else:
                id_objs[id] = obj
                LOGGER.debug("Cached %r %r", type_name, id)
            # If we've got all children of an object
            if base_pattern is not None and \
               pattern.child and pattern.obj_id_set is None:
                # Put the fact in the cache
                parent_child_pattern = query.Pattern(
                    query.Pattern(None, True, base_type,
                                  {get_parent_id(base_type.name, obj)}),
                    True, pattern.obj_type
                )
                if parent_child_pattern in cached:
                    r = self.pattern_responses[parent_child_pattern]
                    r[type_name].append(obj)
                elif parent_child_pattern not in self.pattern_responses:
                    self.pattern_responses[parent_child_pattern] = {
                        type_name: [obj]
                    }
                    cached.add(parent_child_pattern)

        # If we've just loaded all children of the parent pattern
        if (
            pattern.child
            and base_pattern in self.pattern_responses
            and pattern.obj_id_set is None
        ):
            parent_get_id = data.SCHEMA.types[base_type.name].get_id
            parent_response = self.pattern_responses[base_pattern]
            # For each cached parent object
            for parent_obj in parent_response[base_type.name]:
                # Create pattern for its children
                parent_child_pattern = query.Pattern(
                    query.Pattern(None, True, base_type,
                                  {parent_get_id(parent_obj)}),
                    True, pattern.obj_type
                )
                # If we don't have its children cached
                if parent_child_pattern not in self.pattern_responses:
                    # Store the fact that it has none
                    self.pattern_responses[parent_child_pattern] = {
                        type_name: []
                    }
                    cached.add(parent_child_pattern)

        # For every parent-child relation of this pattern's type
        for child_relation in pattern.obj_type.children.values():
            # If we had a query for all this-type children of our pattern
            sub_pattern = query.Pattern(pattern, True, child_relation.child)
            if sub_pattern in self.pattern_responses:
                # For each object in our response
                for obj in objs:
                    # Create pattern for its children of this type
                    parent_child_pattern = query.Pattern(
                        query.Pattern(None, True, pattern.obj_type,
                                      {get_id(obj)}),
                        True,
                        child_relation.child,
                    )
                    # If we don't have its children cached
                    if parent_child_pattern not in self.pattern_responses:
                        # Store the fact that it has none
                        self.pattern_responses[parent_child_pattern] = {
                            child_relation.child.name: []
                        }
                        cached.add(parent_child_pattern)

        # Add pattern response to the cache
        self.pattern_responses[pattern] = response
        cached.add(pattern)
        # Log cached patterns
        LOGGER.debug("Cached patterns %r", cached)
        if LOGGER.getEffectiveLevel() <= logging.INFO:
            LOGGER.info(
                "Cache has %s",
                ", ".join(
                    tuple(
                        f"{len(id_objs)} {type_name}s"
                        for type_name, id_objs in self.type_id_objs.items()
                        if id_objs
                    )
                    + (f"{len(self.pattern_responses)} patterns",)
                ),
            )
        return response

    def oo_query(self, pattern_set):
        """
        Retrieve raw data for objects specified via a pattern set.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.query.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            raw data of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, query.Pattern) for r in pattern_set)

        # Start with an empty response
        response_type_id_objs = {}

        # For each pattern
        for pattern in pattern_set:
            # Try to get the response from the cache
            try:
                pattern_response = self.pattern_responses[pattern]
                LOGGER.debug("Fetched from the cache: %r", pattern)
            # If not found
            except KeyError:
                # Query the source and merge the response into the cache
                pattern_response = self._merge_pattern_response(
                    pattern, self.source.oo_query({pattern})
                )
                LOGGER.debug("Merged into the cache: %r", pattern)
            # Merge into the overall response
            for type_name, objs in pattern_response.items():
                get_id = data.SCHEMA.types[type_name].get_id
                id_objs = response_type_id_objs.get(type_name, {})
                for obj in objs:
                    id_objs[get_id(obj)] = obj
                response_type_id_objs[type_name] = id_objs

        # Return merged and validated response
        response = {
            type_name: list(id_objs.values())
            for type_name, id_objs in response_type_id_objs.items()
        }
        assert LIGHT_ASSERTS or data.SCHEMA.is_valid(response)
        return response
