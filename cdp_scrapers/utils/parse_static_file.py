import json
from logging import getLogger
from pathlib import Path
from typing import Any, Dict


from cdp_backend.pipeline.ingestion_models import (
    Body,
    Person,
    Seat,
)

from .parse_static_person import parse_static_person

from ..types import ScraperStaticData

###############################################################################

log = getLogger(__name__)

###############################################################################


def parse_static_file(file_path: Path) -> ScraperStaticData:
    """
    Parse Seats, Bodies and Persons from static data JSON

    Parameters
    ----------
    file_path: Path
        Path to file containing static data in JSON

    Returns
    -------
    ScraperStaticData:
        Tuple[Dict[str, Seat], Dict[str, Body], Dict[str, Person]]

    See Also
    -----
    parse_static_person()
    sanitize_roles()

    Notes
    -----
    Function looks for "seats", "primary_bodies", "persons" top-level keys
    """
    with open(file_path) as static_file:
        static_json: Dict[str, Dict[str, Any]] = json.load(static_file)

        if "seats" not in static_json:
            seats: Dict[str, Seat] = {}
        else:
            seats: Dict[str, Seat] = {
                seat_name: Seat.from_dict(seat)
                for seat_name, seat in static_json["seats"].items()
            }

        if "primary_bodies" not in static_json:
            primary_bodies: Dict[str, Body] = {}
        else:
            primary_bodies: Dict[str, Body] = {
                body_name: Body.from_dict(body)
                for body_name, body in static_json["primary_bodies"].items()
            }

        if "persons" not in static_json:
            known_persons: Dict[str, Person] = {}
        else:
            known_persons: Dict[str, Person] = {
                person_name: parse_static_person(person, seats, primary_bodies)
                for person_name, person in static_json["persons"].items()
            }

        log.debug(
            f"ScraperStaticData parsed from {file_path}:\n"
            f"    seats: {list(seats.keys())}\n"
            f"    primary_bodies: {list(primary_bodies.keys())}\n"
            f"    persons: {list(known_persons.keys())}\n"
        )
        return ScraperStaticData(
            seats=seats, primary_bodies=primary_bodies, persons=known_persons
        )
