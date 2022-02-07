"""Kernel CI message queue"""

import math
import datetime
import json
import logging
import threading
import sys
import argparse
import email
import email.message
import email.policy
from abc import ABC, abstractmethod
from google.cloud import pubsub
from google.api_core.exceptions import DeadlineExceeded
import kcidb.io as io
import kcidb.orm
from kcidb import misc
from kcidb.misc import LIGHT_ASSERTS


# Module's logger
LOGGER = logging.getLogger(__name__)


class Publisher(ABC):
    """Abstract message queue publisher"""

    @staticmethod
    @abstractmethod
    def encode_data(data):
        """
        Encode published data.

        Args:
            data:   The data to be encoded.

        Returns:
            The encoded message data.

        Raises:
            An exception in case data encoding failed.
        """

    def __init__(self, project_id, topic_name, client=None):
        """
        Initialize a message queue publisher.

        Args:
            project_id:         ID of the Google Cloud project to which the
                                message queue belongs.
            topic_name:         Name of the message queue topic to publish to.
            client:             The Google Pub/Sub PublisherClient to use, or
                                None to create and use one with default
                                settings.
        """
        assert client is None or isinstance(client, pubsub.PublisherClient)
        limit_exceeded_behavior = pubsub.types.LimitExceededBehavior.BLOCK
        self.client = client or pubsub.PublisherClient(
            publisher_options=pubsub.types.PublisherOptions(
                enable_message_ordering=False,
                flow_control=pubsub.types.PublishFlowControl(
                    limit_exceeded_behavior=limit_exceeded_behavior,
                ),
            ),
        )
        self.topic_path = self.client.topic_path(project_id, topic_name)

    def init(self):
        """
        Initialize publishing setup.
        """
        self.client.create_topic(name=self.topic_path)

    def cleanup(self):
        """
        Cleanup publishing setup.
        """
        self.client.delete_topic(topic=self.topic_path)

    def future_publish(self, data):
        """
        Publish data to the message queue in the future.

        Args:
            data:   The JSON data to publish to the message queue.
                    Must adhere to a version of I/O schema.

        Returns:
            A "future" representing the publishing result, returning the
            publishing ID string.
        """
        return self.client.publish(topic=self.topic_path,
                                   data=self.encode_data(data))

    def publish(self, data):
        """
        Publish data to the message queue.

        Args:
            data:   The JSON data to publish to the message queue.
                    Must adhere to a version of I/O schema.

        Returns:
            Publishing ID string.
        """
        return self.future_publish(data).result()

    def publish_iter(self, data_iter, done_cb=None):
        """
        Publish data returned by an iterator.

        Args:
            data_iter:  An iterator returning the JSON data to publish to the
                        message queue. Each must adhere to a version of I/O
                        schema.
            done_cb:    A function to call when a JSON data is successfully
                        published. Will be called with the publishing ID of
                        each data returned by the iterator, in order.
        """
        if done_cb is None:
            def done_cb(publishing_id):
                pass

        futures = []
        futures_lock = threading.Lock()

        def done(_):
            """
            Report and remove all initial completed futures.
            """
            with futures_lock:
                idx = 0
                for idx, future in enumerate(futures):
                    if not future.done():
                        break
                for future in futures[0:idx]:
                    done_cb(future.result())
                del futures[0:idx]

        try:
            # Queue submission futures for each supplied data
            for data in data_iter:
                future = self.future_publish(data)
                with futures_lock:
                    futures.append(future)
                future.add_done_callback(done)
        finally:
            # Grab remaining futures
            remaining_futures = []
            with futures_lock:
                remaining_futures = futures[:]
                del futures[:]

            # Wait for and report remaining futures
            for future in remaining_futures:
                done_cb(future.result())


class Subscriber(ABC):
    """Abstract message queue subscriber"""

    @staticmethod
    @abstractmethod
    def decode_data(message_data):
        """
        Decode message data to extract the original published data.

        Args:
            message_data:   The message data from the message queue
                            ("data" field of pubsub.types.PubsubMessage) to be
                            decoded.

        Returns
            The decoded data.

        Raises:
            An exception in case data decoding failed.
        """

    def __init__(self, project_id, topic_name, subscription_name,
                 client=None):
        """
        Initialize a message queue subscriber.

        Args:
            project_id:         ID of the Google Cloud project to which the
                                message queue belongs.
            topic_name:         Name of the message queue topic to subscribe
                                to.
            subscription_name:  Name of the subscription to use.
            client:             The Google Pub/Sub SubscriberClient to use, or
                                None to create and use one with default
                                settings.
        """
        assert client is None or isinstance(client, pubsub.SubscriberClient)
        self.client = client or pubsub.SubscriberClient()
        self.subscription_path = \
            self.client.subscription_path(project_id, subscription_name)
        self.topic_path = self.client.topic_path(project_id, topic_name)

    def init(self):
        """
        Initialize subscription setup.
        """
        self.client.create_subscription(name=self.subscription_path,
                                        topic=self.topic_path)

    def cleanup(self):
        """
        Cleanup subscription setup.
        """
        self.client.delete_subscription(subscription=self.subscription_path)

    def pull_iter(self, max_num=math.inf, timeout=math.inf):
        """
        Create a generator iterator pulling published data from the message
        queue, discarding (and logging as errors) invalid data. The generator
        stops pulling when either the specified number of messages is
        retrieved or the specified timeout expires.

        Args:
            max_num:    Maximum number of messages to pull, or infinity for
                        unlimited number of messages. Default is infinity.
            timeout:    A finite number representing the seconds to spend
                        retrieving "max_num" messages, or infinity to wait
                        for messages forever. Default is infinity.

        Returns:
            A generator iterator returning messages - tuples, each with two
            items:
            * The ID to use when acknowledging the reception of the data.
            * The decoded data from the message queue.
        """
        assert isinstance(max_num, (int, float))
        assert isinstance(timeout, (int, float))
        received_num = 0
        start_time = None
        while True:
            # Get the current time
            current_time = datetime.datetime.now(datetime.timezone.utc)
            if start_time is None:
                start_time = current_time

            # Check we're within budget
            elapsed_seconds = (current_time - start_time).total_seconds()
            if received_num >= max_num:
                LOGGER.debug("Received enough messages, stopping pulling")
                break
            if elapsed_seconds >= timeout:
                LOGGER.debug("Ran out of time, stopping pulling")
                break

            # Try getting messages
            try:
                # Cap messages/timeout to something PubSub API can handle
                pull_max_messages = int(min(max_num - received_num, 256))
                pull_timeout = min(timeout - elapsed_seconds, 3600)
                LOGGER.debug("Pulling <= %u messages, "
                             "with timeout %us...",
                             pull_max_messages, pull_timeout)
                response = self.client.pull(
                    subscription=self.subscription_path,
                    max_messages=pull_max_messages,
                    timeout=pull_timeout
                )
                messages = response.received_messages
                LOGGER.debug("Pulled %u messages", len(messages))
            except DeadlineExceeded:
                LOGGER.debug("Deadline exceeded")
                messages = ()

            # Yield received message data
            i = 0
            try:
                for i, message in enumerate(messages):
                    try:
                        data = self.decode_data(message.message.data)
                    # This is good enough for now,
                    # pylint: disable=broad-except
                    except Exception as err:
                        LOGGER.error("%s\nFailed decoding, ACK'ing and "
                                     "dropping message:\n%s",
                                     misc.format_exception_stack(err),
                                     message.message.data)
                        self.ack(message.ack_id)
                        continue
                    received_num += 1
                    yield (message.ack_id, data)
            finally:
                # Skip the last-processed message
                i += 1
                # NACK unprocessed messages, if any
                if i < len(messages):
                    self.client.modify_ack_deadline(
                        subscription=self.subscription_path,
                        ack_ids=[m.ack_id for m in messages[i:]],
                        ack_deadline_seconds=0
                    )
                    LOGGER.debug("NACK'ed %s messages", len(messages) - i)

    def pull(self, max_num=1, timeout=math.inf):
        """
        Pull published data from the message queue, discarding (and logging as
        errors) invalid data. Stop pulling when either the specified number of
        messages is retrieved or the specified timeout expires.

        Args:
            max_num:    Maximum number of messages to pull, or infinity for
                        unlimited number of messages. Cannot be infinity, if
                        "timeout" is infinity. Default is infinity.
            timeout:    A finite number representing the seconds to spend
                        retrieving "max_num" messages, or infinity to wait
                        for messages forever. Default is infinity.

        Returns:
            A list of messages - tuples, each with two items:
            * The ID to use when acknowledging the reception of the data.
            * The decoded data from the message queue.
        """
        assert isinstance(max_num, (int, float))
        assert isinstance(timeout, (int, float))
        assert max_num != math.inf or timeout != math.inf
        return list(self.pull_iter(max_num, timeout))

    def ack(self, ack_id):
        """
        Acknowledge reception of data.

        Args:
            ack_id: The ID received with the data to be acknowledged.
        """
        self.client.acknowledge(subscription=self.subscription_path,
                                ack_ids=[ack_id])

    def nack(self, ack_id):
        """
        Signal data wasn't received.

        Args:
            ack_id: The ID received with the data to be marked not received.
        """
        self.client.modify_ack_deadline(subscription=self.subscription_path,
                                        ack_ids=[ack_id],
                                        ack_deadline_seconds=0)


class IOPublisher(Publisher):
    """I/O data queue publisher"""

    @staticmethod
    def encode_data(data):
        """
        Encode JSON data, adhering to the current version of I/O schema, into
        message data.

        Args:
            data:   JSON data to be encoded, adhering to the current I/O schema
                    version.

        Returns
            The encoded message data.

        Raises:
            An exception in case data encoding failed.
        """
        if not LIGHT_ASSERTS:
            io.SCHEMA.validate_exactly(data)
        return json.dumps(data).encode()


class IOSubscriber(Subscriber):
    """I/O data queue subscriber"""

    @staticmethod
    def decode_data(message_data):
        """
        Decode message data to extract the JSON data adhering to the current
        I/O schema.

        Args:
            message_data:   The message data from the message queue
                            ("data" field of pubsub.types.PubsubMessage) to be
                            decoded.

        Returns
            The decoded JSON data adhering to the current I/O schema.

        Raises:
            An exception in case data decoding failed.
        """
        data = json.loads(message_data.decode())
        return io.SCHEMA.upgrade(io.SCHEMA.validate(data), copy=False)


class ORMPatternPublisher(Publisher):
    """ORM pattern queue publisher"""

    @staticmethod
    def encode_data(data):
        """
        Encode a set of kcidb.orm.Pattern objects, into message data.

        Args:
            data:   The set to encode.

        Returns
            The encoded message data.

        Raises:
            An exception in case data encoding failed.
        """
        assert isinstance(data, set)
        assert all(isinstance(pattern, kcidb.orm.Pattern)
                   for pattern in data)
        return "".join(
            repr(pattern) + "\n" for pattern in data
        ).encode()


class ORMPatternSubscriber(Subscriber):
    """ORM pattern queue subscriber"""

    @staticmethod
    def decode_data(message_data):
        """
        Decode message data to extract kcidb.orm.Pattern objects.

        Args:
            message_data:   The message data from the message queue
                            ("data" field of pubsub.types.PubsubMessage) to be
                            decoded.

        Returns
            The decoded set of kcidb.orm.Pattern objects.

        Raises:
            An exception in case data decoding failed.
        """
        pattern_set = set()
        for line in message_data.decode().splitlines():
            pattern_set |= kcidb.orm.Pattern.parse(line)
        return pattern_set


class EmailPublisher(Publisher):
    """Email queue publisher"""

    @staticmethod
    def encode_data(data):
        """
        Encode an email, into message data.

        Args:
            data:   The email to encode.

        Returns
            The encoded message data.

        Raises:
            An exception in case data encoding failed.
        """
        assert isinstance(data, email.message.EmailMessage)
        return data.as_string(policy=email.policy.SMTPUTF8).encode()


class EmailSubscriber(Subscriber):
    """Email queue subscriber"""

    def decode_data(self, message_data):
        """
        Decode email from the message data.

        Args:
            message_data:   The message data from the message queue
                            ("data" field of pubsub.types.PubsubMessage) to be
                            decoded.

        Returns
            The decoded email (email.message.EmailMessage) object.

        Raises:
            An exception in case data decoding failed.
        """
        return self.parser.parsestr(message_data.decode())

    def __init__(self, *args, **kwargs):
        """
        Initialize the email subscriber.

        Args:
            args:   The positional arguments to initialize Subscriber with.
            kwargs: The keyword arguments to initialize Subscriber with.
        """
        super().__init__(*args, **kwargs)
        self.parser = email.parser.Parser(policy=email.policy.SMTPUTF8)


def argparse_add_args(parser):
    """
    Add common message queue arguments to an argument parser.

    Args:
        parser:     The parser to add arguments to.
    """
    parser.add_argument(
        '-p', '--project',
        help='ID of the Google Cloud project with the message queue',
        required=True
    )
    parser.add_argument(
        '-t', '--topic',
        help='Name of the message queue topic',
        required=True
    )


class ArgumentParser(misc.ArgumentParser):
    """
    Command-line argument parser with common message queue arguments added.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding common message queue arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        argparse_add_args(self)


def argparse_publisher_add_args(parser, data_name):
    """
    Add message queue publisher arguments to an argument parser.

    Args:
        parser:     The parser to add arguments to.
        data_name:  Name of the message queue data.
    """
    argparse_add_args(parser)
    subparsers = parser.add_subparsers(dest="command",
                                       title="Available commands",
                                       metavar="COMMAND",
                                       parser_class=argparse.ArgumentParser)
    subparsers.required = True
    parser.subparsers = {}
    description = f"Initialize {data_name} publisher setup"
    parser.subparsers["init"] = subparsers.add_parser(
        name="init", help=description, description=description
    )
    description = \
        f"Publish {data_name} from standard input, print publishing IDs"
    parser.subparsers["publish"] = subparsers.add_parser(
        name="publish", help=description, description=description
    )
    description = f"Cleanup {data_name} publisher setup"
    parser.subparsers["cleanup"] = subparsers.add_parser(
        name="cleanup", help=description, description=description
    )


class PublisherArgumentParser(misc.ArgumentParser):
    """
    Command-line argument parser with common message queue arguments added.
    """

    def __init__(self, data_name, *args, **kwargs):
        """
        Initialize the parser, adding common message queue arguments.

        Args:
            data_name:  Name of the message queue data.
            args:       Positional arguments to initialize ArgumentParser
                        with.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        self.subparsers = {}
        argparse_publisher_add_args(self, data_name)


def argparse_subscriber_add_args(parser, data_name):
    """
    Add message queue subscriber arguments to an argument parser.

    Args:
        parser:     The parser to add arguments to.
        data_name:  Name of the message queue data.
    """
    argparse_add_args(parser)
    parser.add_argument(
        '-s', '--subscription',
        help='Name of the subscription',
        required=True
    )
    subparsers = parser.add_subparsers(dest="command",
                                       title="Available commands",
                                       metavar="COMMAND",
                                       parser_class=argparse.ArgumentParser)
    subparsers.required = True
    parser.subparsers = {}

    description = f"Initialize {data_name} subscriber setup"
    parser.subparsers["init"] = subparsers.add_parser(
        name="init", help=description, description=description
    )

    description = \
        f"Pull {data_name} with a subscriber, print to standard output"
    parser.subparsers["pull"] = subparsers.add_parser(
        name="pull", help=description, description=description
    )
    parser.subparsers["pull"].add_argument(
        '--timeout',
        metavar="SECONDS",
        type=float,
        help='Wait the specified number of SECONDS for a message, '
             'or "inf" for infinity. Default is "inf".',
        default=math.inf,
        required=False
    )
    parser.subparsers["pull"].add_argument(
        '-m',
        '--messages',
        metavar="NUMBER",
        type=misc.non_negative_int_or_inf,
        help='Pull maximum NUMBER of messages, or "inf" for infinity. '
             'Default is 1.',
        default=1,
        required=False
    )

    description = f"Cleanup {data_name} subscriber setup"
    parser.subparsers["cleanup"] = subparsers.add_parser(
        name="cleanup", help=description, description=description
    )


class SubscriberArgumentParser(misc.ArgumentParser):
    """
    Command-line argument parser with message queue subscriber arguments
    added.
    """

    def __init__(self, data_name, *args, **kwargs):
        """
        Initialize the parser, adding message queue subscriber arguments.

        Args:
            data_name:  Name of the message queue data.
            args:       Positional arguments to initialize ArgumentParser
                        with.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        self.subparsers = {}
        argparse_subscriber_add_args(self, data_name)


def io_publisher_main():
    """Execute the kcidb-mq-io-publisher command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = \
        'kcidb-mq-io-publisher - ' \
        'Kernel CI I/O data publisher management tool'
    parser = PublisherArgumentParser("I/O data", description=description)
    args = parser.parse_args()
    publisher = IOPublisher(args.project, args.topic)
    if args.command == "init":
        publisher.init()
    elif args.command == "cleanup":
        publisher.cleanup()
    elif args.command == "publish":
        def print_publishing_id(publishing_id):
            print(publishing_id, file=sys.stdout)
            sys.stdout.flush()
        publisher.publish_iter(
            (io.SCHEMA.upgrade(io.SCHEMA.validate(data), copy=False)
             for data in misc.json_load_stream_fd(sys.stdin.fileno())),
            done_cb=print_publishing_id
        )


def io_subscriber_main():
    """Execute the kcidb-mq-io-subscriber command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = \
        'kcidb-mq-io-subscriber - ' \
        'Kernel CI I/O data subscriber management tool'
    parser = SubscriberArgumentParser("I/O data", description=description)
    misc.argparse_output_add_args(parser.subparsers["pull"])
    args = parser.parse_args()
    subscriber = IOSubscriber(args.project, args.topic, args.subscription)
    if args.command == "init":
        subscriber.init()
    elif args.command == "cleanup":
        subscriber.cleanup()
    elif args.command == "pull":
        for ack_id, data in \
                subscriber.pull_iter(args.messages, timeout=args.timeout):
            misc.json_dump(data, sys.stdout, indent=args.indent, seq=args.seq)
            sys.stdout.flush()
            subscriber.ack(ack_id)


def pattern_publisher_main():
    """Execute the kcidb-mq-pattern-publisher command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = \
        'kcidb-mq-pattern-publisher - ' \
        'Kernel CI ORM pattern publisher management tool'
    parser = PublisherArgumentParser("ORM patterns", description=description)
    parser.subparsers["publish"].add_argument(
        '--pattern-help',
        action=kcidb.orm.PatternHelpAction,
        help='Print pattern string documentation and exit.'
    )
    args = parser.parse_args()
    publisher = ORMPatternPublisher(args.project, args.topic)
    if args.command == "init":
        publisher.init()
    elif args.command == "cleanup":
        publisher.cleanup()
    elif args.command == "publish":
        pattern_set = set()
        for line_idx, line in enumerate(sys.stdin):
            try:
                pattern_set |= kcidb.orm.Pattern.parse(line)
            except Exception as exc:
                raise Exception(
                    f"Failed parsing ORM pattern on line {line_idx + 1}: "
                    f"{line!r}"
                ) from exc
        print(publisher.publish(pattern_set))


def pattern_subscriber_main():
    """Execute the kcidb-mq-pattern-subscriber command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = \
        'kcidb-mq-pattern-subscriber - ' \
        'Kernel CI ORM pattern subscriber management tool'
    parser = SubscriberArgumentParser("ORM patterns", description=description)
    args = parser.parse_args()
    subscriber = ORMPatternSubscriber(args.project, args.topic,
                                      args.subscription)
    if args.command == "init":
        subscriber.init()
    elif args.command == "cleanup":
        subscriber.cleanup()
    elif args.command == "pull":
        for ack_id, data in \
                subscriber.pull_iter(args.messages, timeout=args.timeout):
            sys.stdout.write("".join(
                repr(pattern) + "\n" for pattern in data
            ))
            sys.stdout.flush()
            subscriber.ack(ack_id)
