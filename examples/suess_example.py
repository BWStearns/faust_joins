import unittest
import uuid
import logging
from typing import Optional, AsyncIterable

import faust

from joins.joiner import make_joining_func

app = faust.App("suess", broker="kafka://localhost")

join_waitlist = app.Table("join_waitlist", partitions=1)

def things_both_there(suess_thing):
    """Check that the message has requisite elements."""
    # Figure out which things the test_message actually needs.
    return all([suess_thing.thing_one, suess_thing.thing_two])


def if_insufficient(suess_thing):
    """Placeholder function for incomplete things."""
    logging.info("INSUFFICIENT: " + str(suess_thing))


def do_things_to_the_thing(suess_thing):
    """To keep joiner logic separate from the suess logic just putting a stub here."""
    # Insert the messages into a DB here
    logging.info("Doing things with: " + str(suess_thing))

def merge_things(t1, t2):
    return TestMessageFormat(
        thing_id=t1.thing_id,
        thing_one=(t1.thing_one or t2.thing_one),
        thing_two=(t1.thing_two or t2.thing_two),
    )

class TestMessageFormat(faust.Record, serializer="json"):
    thing_id: uuid.UUID
    thing_one: Optional[str]
    thing_two: Optional[str]

suess_processor = make_joining_func(
    tbl=join_waitlist,
    key_fn=(lambda r: r.thing_id),
    merge_fn=merge_things,
    sufficiency_fn=things_both_there,
    process_fn=do_things_to_the_thing,
    handle_incomplete_fn=if_insufficient,
)

suess_topic = app.topic("suess", value_type=TestMessageFormat)

@app.agent(suess_topic)
async def my_join(suess_things: AsyncIterable[TestMessageFormat]) -> None:
    """Joins messages from multiple contributing agents.

    Expects all fields to be non-None in the combined message (falsey values are fine,
    just not None).
    """
    async for suess_thing in suess_things:
        suess_processor(suess_thing)
