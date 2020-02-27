from typing import Callable, Optional, Union

import faust


class TableException(Exception):
    def __init__(self, *args, **kwargs):
        """Throw TableException."""
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        """Make a string representation of TableException."""
        if self.message:
            return f"Faust Table exception: {self.message}"
        return "Faust Table exception."


class TableJoinDefaultException(TableException):
    def __init__(self, *args, **kwargs):
        """Throw TableJoinDefaultException.

        Defaults aren't allowed for joins because of how Faust Tables, Mode, and Typing handle
        which __missing__ method gets used on collections and dictionary-like objects when a
        __get__ call fails. The end result is that Faust Tables defer to a __missing__ method
        that prioritizes the table default over the caller supplied default. Ideally it wouldn't
        matter and the caller default would be prioritized, but that's now how it works currently.
        """
        self.message = "Table used in a join has a default value which will break joining logic. \
                        Ensure Table default is set to `None`"


def make_joining_func(
    tbl: Union[faust.Table, list],
    key_fn: Callable,
    merge_fn: Callable,
    sufficiency_fn: Callable,
    process_fn: Callable,
    handle_incomplete_fn: Optional[Callable],
) -> Callable:
    """Handle the Joining logic for agents.

    Create a function that handles joining streams. The caller needs to provide a faust Table and
    four functions, with an optional fifth. The table should not have a default value.

    Messages can be whatever types are handled by the caller provider functions, but this logic
    was written mostly with Faust Record types in mind.

    Messages are stored in a Table, when new messages with the same key come in they are merged.
    Then they are checked by the sufficiency_fn, and if True, then they are processed with the
    process_fn. You can optionally pass an handle_incomplete_fn which will execute on updated but
    incomplete messages.

    When a message is processed it is then removed from the table. There is currently no logic for
    removing incomplete messages from the table.

    Args:
        tbl: Table, this will hold the messages until they are complete and processed.
        key_fn: Callable, takes a message and returns some hashable for use as a key.
        merge_fn: Callable, combines two messages and returns a message.
        sufficiency_fn: Callable, takes a message and returns True if message is complete.
        process_fn: Callable, performs business logic on a message if message is complete.
        handle_incomplete_fn: Optional Callable, run on an updated message if it is incomplete.

    Returns:
        Callable meant for use on messages inside a Faust agent.
    """
    if tbl.default is not None:
        raise TableJoinDefaultException
    incomplete_handler = handle_incomplete_fn or (lambda r: None)

    def processor_function(new_message):
        k = key_fn(new_message)
        extant_message = tbl.get(k, new_message)
        tbl[k] = merge_fn(new_message, extant_message)
        updated_message = tbl[k]
        if sufficiency_fn(updated_message):
            processed = process_fn(updated_message)
            tbl.pop(k)
            return processed
        return incomplete_handler(updated_message)

    return processor_function
