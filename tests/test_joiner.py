import unittest
import uuid
import logging

from faust import Record

from joins.joiner import make_joining_func, TableJoinDefaultException


def things_both_there(record):
    """Check that the message has requisite elements."""
    # Figure out which things peristence actually needs.
    return all([record.thing_one, record.thing_two])


def if_insufficient(record):
    """Placeholder function for incomplete things."""
    logging.debug("INSUFFICIENT: " + str(record))


def do_things_to_the_thing(record):
    """To keep joiner logic separate from the persistence logic just putting a stub here."""
    # Insert the messages into a DB here
    logging.debug("Doing things with: " + str(record))

class DummyTable(dict):
    default = None

class TestMessageFormat(Record, serializer="json"):
    thing_id: uuid.UUID
    thing_one: str
    thing_two: str

def merge_things(t1, t2):
    return TestMessageFormat(
        thing_id=t1.thing_id,
        thing_one=(t1.thing_one or t2.thing_one),
        thing_two=(t1.thing_two or t2.thing_two),
    )

m1_uuid = uuid.uuid4()
message_1_1 = TestMessageFormat(m1_uuid, "one fish", None)
message_1_2  = TestMessageFormat(m1_uuid, None, "two fish")

m2_uuid = uuid.uuid4()
message_2_1 = TestMessageFormat(m2_uuid, "sam",  None)
message_2_2 = TestMessageFormat(m2_uuid, None, "I am")


class TestJoins(unittest.TestCase):
    def test_inner_joins(self):

        dummy_tbl = DummyTable()

        dummy_tbl.default = {}

        with self.assertRaises(TableJoinDefaultException):
            make_joining_func(
                tbl=dummy_tbl,
                key_fn=(lambda r: r.thing_id),
                merge_fn=merge_things,
                sufficiency_fn=things_both_there,
                process_fn=do_things_to_the_thing,
                handle_incomplete_fn=if_insufficient,
            )

        dummy_tbl.default = None

        terminal_db_processor = make_joining_func(
            tbl=dummy_tbl,
            key_fn=(lambda r: r.thing_id),
            merge_fn=merge_things,
            sufficiency_fn=things_both_there,
            process_fn=do_things_to_the_thing,
            handle_incomplete_fn=if_insufficient,
        )

        # Start Empty
        self.assertEqual(len(dummy_tbl), 0)

        # PROCESING A THING
        terminal_db_processor(message_1_1)

        
        # Added thing 1 part 1, There should be one record in the table
        self.assertEqual(len(dummy_tbl), 1)

        # Still only one record in the table since they're for the same thing
        self.assertEqual(len(dummy_tbl), 1)

        # PROCESING A THING
        terminal_db_processor(message_2_1)

        # Added a part from a second thing, so now we have two things in the table.
        self.assertEqual(len(dummy_tbl), 2)

        # PROCESING A THING
        terminal_db_processor(message_1_2)

        # PROCESING A THING
        terminal_db_processor(message_2_2)

        # All done!
        self.assertEqual(len(dummy_tbl), 0)
