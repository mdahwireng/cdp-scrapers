import re
from datetime import datetime, timedelta
from itertools import filterfalse, groupby
from typing import List, NamedTuple, Optional

import pytz
from cdp_backend.database.constants import RoleTitle
from cdp_backend.pipeline.ingestion_models import Role

from .str_simplified import str_simplified

from ..types import ScraperStaticData


def sanitize_roles(
    person_name: str,
    roles: Optional[List[Role]] = None,
    static_data: Optional[ScraperStaticData] = None,
    council_pres_patterns: List[str] = ["chair", "pres", "super"],
    chair_patterns: List[str] = ["chair", "pres"],
) -> Optional[List[Role]]:
    """
    1. Standardize roles[i].title to RoleTitle constants
    2. Ensure only 1 councilmember Role per term

    Parameters
    ----------
    person_name: str
        Sanitization target Person.name

    roles: Optional[List[Role]] = None
        target Person's Roles to sanitize

    static_data: Optional[ScraperStaticData]
        Static data defining primary council bodies and predefined Person.seat.roles.
        See Notes.

    council_pres_patterns: List[str]
        Set roles[i].title as "Council President" if match
        and roles[i].body is a primary body like City Council
    chair_patterns: List[str]
        Set roles[i].title as "Chair" if match
        and roles[i].body is not a primary body

    Notes
    -----
    Remove roles[#] if roles[#].body in static_data.primary_bodies.
    Use static_data.persons[#].seat.roles instead.

    If roles[i].body not in static_data.primary_bodies,
    roles[i].title cannot be "Councilmember" or "Council President".

    Use "City Council" and "Council Briefing"
    if static_data.primary_bodies is empty.
    """
    if roles is None:
        roles = []

    if not static_data or not static_data.primary_bodies:
        # Primary/full council not defined in static data file.
        # these are reasonably good defaults for most municipalities.
        primary_body_names = ["city council", "council briefing"]
    else:
        primary_body_names = [
            body_name.lower() for body_name in static_data.primary_bodies.keys()
        ]

    try:
        have_primary_roles = len(static_data.persons[person_name].seat.roles) > 0
    except (KeyError, AttributeError, TypeError):
        have_primary_roles = False

    def _is_role_period_ok(role: Role) -> bool:
        """
        Test that role.[start | end]_datetime is acceptable
        """
        if role.start_datetime is None or role.end_datetime is None:
            return False
        if not have_primary_roles:
            # no roles in static data; accept if this this role is current
            return role.start_datetime.astimezone(
                pytz.utc
            ) <= datetime.today().astimezone(pytz.utc) and datetime.today().astimezone(
                pytz.utc
            ) <= role.end_datetime.astimezone(
                pytz.utc
            )
        # accept if role coincides with one given in static data
        for static_role in static_data.persons[person_name].seat.roles:
            if (
                static_role.start_datetime <= role.start_datetime
                and role.end_datetime <= static_role.end_datetime
            ):
                return True
        return False

    def _is_primary_body(role: Role) -> bool:
        """
        Is role.body one of primary_bodies in static data file
        """
        return (
            role.body is not None
            and role.body.name is not None
            and str_simplified(role.body.name).lower() in primary_body_names
        )

    def _fix_primary_title(role: Role) -> str:
        """
        Council president or Councilmember
        """
        if (
            role.title is None
            or re.search(
                "|".join(council_pres_patterns), str_simplified(role.title), re.I
            )
            is None
        ):
            return RoleTitle.COUNCILMEMBER
        return RoleTitle.COUNCILPRESIDENT

    def _fix_nonprimary_title(role: Role) -> str:
        """
        Not council president or councilmember
        """
        if role.title is None:
            return RoleTitle.MEMBER

        role_title = str_simplified(role.title).lower()
        # Role is not for a primary/full council
        # Role.title cannot be Councilmember or Council President
        if "vice" in role_title:
            return RoleTitle.VICE_CHAIR
        if "alt" in role_title:
            return RoleTitle.ALTERNATE
        if "super" in role_title:
            return RoleTitle.SUPERVISOR
        if re.search("|".join(chair_patterns), role_title, re.I) is not None:
            return RoleTitle.CHAIR
        return RoleTitle.MEMBER

    def _is_councilmember_term(role: Role) -> bool:
        return (
            role.title == RoleTitle.COUNCILMEMBER
            and role.start_datetime is not None
            and role.end_datetime is not None
        )

    roles = list(
        # drop dynamically scraped primary roles
        # if primary roles are given in static data
        filterfalse(
            lambda role: have_primary_roles and _is_primary_body(role),
            # filter out bad start_datetime, end_datetime
            filter(_is_role_period_ok, roles),
        )
    )
    # standardize titles
    for role in filter(_is_primary_body, roles):
        role.title = _fix_primary_title(role)
    for role in filterfalse(_is_primary_body, roles):
        role.title = _fix_nonprimary_title(role)

    class CouncilMemberTerm(NamedTuple):
        start_datetime: datetime
        end_datetime: datetime
        index_in_roles: int

    # when checking for overlapping terms, we should do so per body.
    # e.g. simultaneous councilmember roles in city council and in council briefing
    # are completely acceptable and common.

    scraped_member_roles_by_body: List[List[Role]] = [
        list(roles_for_body)
        for body_name, roles_for_body in groupby(
            sorted(
                filter(
                    # get all dynamically scraped councilmember terms
                    lambda role: not have_primary_roles
                    and _is_councilmember_term(role),
                    roles,
                ),
                # sort from old to new role
                key=lambda role: (
                    role.body.name,
                    role.start_datetime,
                    role.end_datetime,
                ),
            ),
            # group by body
            key=lambda role: role.body.name,
        )
    ]

    if have_primary_roles:
        # don't forget to include info from the static data file
        roles.extend(static_data.persons[person_name].seat.roles)
    if len(scraped_member_roles_by_body) == 0:
        # no Councilmember roles dynamically scraped
        # nothing more to do
        return roles

    for roles_for_body in scraped_member_roles_by_body:
        for i in [i for i, role in enumerate(roles_for_body) if i > 0]:
            prev_role = roles_for_body[i - 1]
            this_role = roles_for_body[i]
            # if member role i overlaps with member role j, end i before j
            if prev_role.end_datetime > this_role.start_datetime:
                roles[
                    roles.index(prev_role)
                ].end_datetime = this_role.start_datetime - timedelta(days=1)

    return roles