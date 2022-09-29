from copy import deepcopy
from logging import getLogger
from typing import Any, Dict, List


from cdp_backend.database.constants import RoleTitle
from cdp_backend.pipeline.ingestion_models import (
    Body,
    Person,
    Role,
    Seat,
)
from cdp_backend.utils.constants_utils import get_all_class_attr_values


###############################################################################

log = getLogger(__name__)

###############################################################################


def parse_static_person(
    person_json: Dict[str, Any],
    all_seats: Dict[str, Seat],
    primary_bodies: Dict[str, Body],
) -> Person:
    """
    Parse Dict[str, Any] for a person in static data file to a Person instance.
    person_json["seat"] and person_json["roles"] are validated against
    all_seats and primary_bodies in static data file.

    Parameters
    ----------
    person_json: Dict[str, Any]
        A dictionary in static data file with info for a Person.

    all_seats: Dict[str, Seat]
        Seats defined as top-level in static data file

    primary_bodies: Dict[str, Body]
        Bodies defined as top-level in static data file.


    See Also
    --------
    parse_static_file()
    sanitize_roles()
    """
    log.debug(f"Begin parsing static data for {person_json['name']}")

    person: Person = Person.from_dict(
        # "seat" and "roles" are not direct serializations of Seat/Role
        {k: v for k, v in person_json.items() if k != "seat" and k != "roles"}
    )
    if "seat" not in person_json:
        log.debug("Seat name not given")
        return person

    seat_name: str = person_json["seat"]
    if seat_name not in all_seats:
        log.error(f"{seat_name} is not defined in top-level 'seats'")
        return person

    # Keep all_seats unmodified; we will append Roles to this person.seat below
    person.seat = deepcopy(all_seats[seat_name])
    if "roles" not in person_json:
        log.debug("Roles not given")
        return person

    # Role.title must be a RoleTitle constant so get all allowed values
    role_titles: List[str] = get_all_class_attr_values(RoleTitle)
    for role_json in person_json["roles"]:
        if (
            # if str, it is looked-up in primary_bodies
            isinstance(role_json["body"], str)
            and role_json["body"] not in primary_bodies
        ):
            log.error(
                f"{role_json} is ignored. "
                f"{role_json['body']} is not defined in top-level 'primary_bodies'"
            )
        elif role_json["title"] not in role_titles:
            log.error(
                f"{role_json} is ignored. "
                f"{role_json['title']} is not a RoleTitle constant."
            )
        else:
            role: Role = Role.from_dict(
                {k: v for k, v in role_json.items() if k != "body"}
            )
            if isinstance(role_json["body"], str):
                role.body = primary_bodies[role_json["body"]]
            else:
                # This role.body is a dictionary and defines a non-primary one
                # e.g. like a committee such as Transportation
                # that is not the main/full council
                role.body = Body.from_dict(role_json["body"])

            if person.seat.roles is None:
                person.seat.roles = [role]
            else:
                person.seat.roles.append(role)

    return person


