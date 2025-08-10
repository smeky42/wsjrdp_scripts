from __future__ import annotations

from ._config import Config
from ._connection import ConnectionContext
from ._payment import PaymentRole, mandate_id_from_hitobito_id

__all__ = [
    "Config",
    "ConnectionContext",
    "PaymentRole",
    "console_confirm",
    "mandate_id_from_hitobito_id",
]

_CONSOLE_CONFIRM_DEFAULT_TO_CHOICE_DISPLAY = {
    None: "y/n",
    True: "Y/n",
    False: "y/N",
}

_CONSOLE_CONFIRM_INPUT_TO_VALUE = {
    "yes": True,
    "ye": True,
    "y": True,
    "no": False,
    "n": False,
}


def console_confirm(question, *, default: bool | None = False) -> bool:
    allowed_choices = _CONSOLE_CONFIRM_DEFAULT_TO_CHOICE_DISPLAY.get(default, "y/n")
    while True:
        raw_user_input = input(f"{question} [{allowed_choices}]? ")
        user_input = raw_user_input.strip().lower()
        if not user_input and default is not None:
            return default
        elif (val := _CONSOLE_CONFIRM_INPUT_TO_VALUE.get(user_input, None)) is not None:
            return val
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n", flush=True)
