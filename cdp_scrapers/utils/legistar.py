#!/usr/bin/env python
# -*- coding: utf-8 -*-

import enum
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, NamedTuple, Optional
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup

from .str_simplified import str_simplified

from ..types import ContentURIs, LegistarContentParser
from ..legistar_content_parsers import all_parsers

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

LEGISTAR_BASE = "http://webapi.legistar.com/v1/{client}"
LEGISTAR_VOTE_BASE = LEGISTAR_BASE + "/EventItems"
LEGISTAR_EVENT_BASE = LEGISTAR_BASE + "/Events"
LEGISTAR_MATTER_BASE = LEGISTAR_BASE + "/Matters"
LEGISTAR_PERSON_BASE = LEGISTAR_BASE + "/Persons"
LEGISTAR_BODY_BASE = LEGISTAR_BASE + "/Bodies"

# e.g. Session.video_uri =  EventVideoPath from legistar api
LEGISTAR_SESSION_VIDEO_URI = "EventVideoPath"
LEGISTAR_EV_MINUTE_DECISION = "EventItemPassedFlagName"
# NOTE: EventItemAgendaSequence is also a candidate for this
LEGISTAR_EV_INDEX = "EventItemMinutesSequence"
LEGISTAR_PERSON_EMAIL = "PersonEmail"
LEGISTAR_PERSON_EXT_ID = "PersonId"
LEGISTAR_PERSON_NAME = "PersonFullName"
LEGISTAR_PERSON_PHONE = "PersonPhone"
LEGISTAR_PERSON_WEBSITE = "PersonWWW"
LEGISTAR_PERSON_ACTIVE = "PersonActiveFlag"
LEGISTAR_PERSON_ROLES = "OfficeRecordInfo"
LEGISTAR_BODY_NAME = "BodyName"
LEGISTAR_BODY_EXT_ID = "BodyId"
LEGISTAR_BODY_ACTIVE = "BodyActiveFlag"
LEGISTAR_VOTE_DECISION = "VoteResult"
LEGISTAR_VOTE_EXT_ID = "VoteId"
LEGISTAR_FILE_EXT_ID = "MatterAttachmentId"
LEGISTAR_FILE_NAME = "MatterAttachmentName"
LEGISTAR_FILE_URI = "MatterAttachmentHyperlink"
LEGISTAR_MATTER_EXT_ID = "EventItemMatterId"
LEGISTAR_MATTER_TITLE = "EventItemMatterFile"
LEGISTAR_MATTER_NAME = "EventItemMatterName"
LEGISTAR_MATTER_TYPE = "EventItemMatterType"
LEGISTAR_MATTER_STATUS = "EventItemMatterStatus"
LEGISTAR_MATTER_SPONSORS = "MatterSponsorInfo"
LEGISTAR_SPONSOR_PERSON = "SponsorPersonInfo"
# Session.session_datetime is a combo of EventDate and EventTime
# TODO: this means same time for all Sessions in a EventIngestionModel.
#       some other legistar api data that can be used instead
LEGISTAR_SESSION_DATE = "EventDate"
LEGISTAR_SESSION_TIME = "EventTime"
LEGISTAR_AGENDA_URI = "EventAgendaFile"
LEGISTAR_MINUTES_URI = "EventMinutesFile"
LEGISTAR_MINUTE_EXT_ID = "EventItemId"
LEGISTAR_MINUTE_NAME = "EventItemTitle"
LEGISTAR_VOTE_VAL_ID = "VoteValueId"
LEGISTAR_VOTE_VAL_NAME = "VoteValueName"
LEGISTAR_ROLE_BODY = "OfficeRecordBodyInfo"
LEGISTAR_ROLE_BODY_ALT = "OfficeRecordBodyName"
LEGISTAR_ROLE_START = "OfficeRecordStartDate"
LEGISTAR_ROLE_END = "OfficeRecordEndDate"
LEGISTAR_ROLE_EXT_ID = "OfficeRecordId"
LEGISTAR_ROLE_TITLE = "OfficeRecordTitle"
LEGISTAR_ROLE_TITLE_ALT = "OfficeRecordMemberType"

LEGISTAR_EV_ITEMS = "EventItems"
LEGISTAR_EV_ATTACHMENTS = "EventItemMatterAttachments"
LEGISTAR_EV_VOTES = "EventItemVoteInfo"
LEGISTAR_VOTE_PERSONS = "PersonInfo"
LEGISTAR_EV_SITE_URL = "EventInSiteURL"
LEGISTAR_EV_EXT_ID = "EventId"
LEGISTAR_EV_BODY = "EventBodyInfo"

LEGISTAR_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
###############################################################################


known_legistar_persons: Dict[int, Dict[str, Any]] = {}
known_legistar_bodies: Dict[int, Dict[str, Any]] = {}
# video web page parser type per municipality
video_page_parser: Dict[str, LegistarContentParser] = {}


def get_legistar_body(
    client: str,
    body_id: int,
    use_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Return information for a single legistar body in JSON.

    Parameters
    ----------
    client: str
        Which legistar client to target. Ex: "seattle"
    body_id: int
        Unique ID for this body in the legistar municipality
    use_cache: bool
        True: Store result to prevent querying repeatedly for same body_id

    Returns
    -------
    body: Dict[str, Any]
        legistar API body

    Notes
    -----
    known_legistar_bodies cache is cleared for every LegistarScraper.get_events() call
    """
    global known_legistar_bodies

    if use_cache:
        try:
            return known_legistar_bodies[body_id]
        except KeyError:
            # new body
            pass

    body_request_format = LEGISTAR_BODY_BASE + "/{body_id}"
    response = requests.get(
        body_request_format.format(
            client=client,
            body_id=body_id,
        )
    )

    if response.status_code == 200:
        body = response.json()
    else:
        body = None

    if use_cache:
        known_legistar_bodies[body_id] = body
    return body


def get_legistar_person(
    client: str,
    person_id: int,
    use_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Return information for a single legistar person in JSON.

    Parameters
    ----------
    client: str
        Which legistar client to target. Ex: "seattle"
    person_id: int
        Unique ID for this person in the legistar municipality
    use_cache: bool
        True: Store result to prevent querying repeatedly for same person_id

    Returns
    -------
    person: Dict[str, Any]
        legistar API person

    Notes
    -----
    known_legistar_persons cache is cleared for every LegistarScraper.get_events() call
    """
    global known_legistar_persons

    if use_cache:
        try:
            return known_legistar_persons[person_id]
        except KeyError:
            # new person
            pass

    person_request_format = LEGISTAR_PERSON_BASE + "/{person_id}"
    response = requests.get(
        person_request_format.format(
            client=client,
            person_id=person_id,
        )
    )

    if response.status_code != 200:
        if use_cache:
            known_legistar_persons[person_id] = None
        return None

    person = response.json()

    # all known OfficeRecords (roles) for this person
    response = requests.get(
        (person_request_format + "/OfficeRecords").format(
            client=client,
            person_id=person_id,
        )
    )

    if response.status_code != 200:
        person[LEGISTAR_PERSON_ROLES] = None
        if use_cache:
            known_legistar_persons[person_id] = person
        return person

    office_records: List[Dict[str, Any]] = response.json()
    for record in office_records:
        # body for this role
        record[LEGISTAR_ROLE_BODY] = get_legistar_body(
            client=client, body_id=record["OfficeRecordBodyId"], use_cache=use_cache
        )

    person[LEGISTAR_PERSON_ROLES] = office_records
    if use_cache:
        known_legistar_persons[person_id] = person
    return person


def get_legistar_events_for_timespan(
    client: str,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> List[Dict]:
    """
    Get all legistar events and each events minutes items, people, and votes, for a
    client for a given timespan.

    Parameters
    ----------
    client: str
        Which legistar client to target. Ex: "seattle"
    begin: Optional[datetime]
        The timespan beginning datetime to query for events after.
        Default: UTC now - 1 day
    end: Optional[datetime]
        The timespan end datetime to query for events before.
        Default: UTC now

    Returns
    -------
    events: List[Dict]
        All legistar events that occur between the datetimes provided for the client
        provided. Additionally, requests and attaches agenda items, minutes items, any
        attachments, called "EventItems", requests votes for any of these "EventItems",
        and requests person information for any vote.
    """
    # Set defaults
    if begin is None:
        begin = datetime.utcnow() - timedelta(days=1)
    if end is None:
        end = datetime.utcnow()

    # The unformatted request parts
    filter_datetime_format = "EventDate+{op}+datetime%27{dt}%27"
    request_format = LEGISTAR_EVENT_BASE + "?$filter={begin}+and+{end}"

    # a given person and/or body's information being updated
    # during the lifetime of this single call is miniscule.
    # use a cache to prevent 10s-100s of web requests
    # for the same person/body
    global known_legistar_persons, known_legistar_bodies
    # See Also
    # get_legistar_person()
    known_legistar_persons.clear()
    # See Also
    # get_legistar_body()
    known_legistar_bodies.clear()

    # Get response from formatted request
    log.debug(f"Querying Legistar for events between: {begin} - {end}")
    response = requests.get(
        request_format.format(
            client=client,
            begin=filter_datetime_format.format(
                op="ge",
                dt=str(begin).replace(" ", "T"),
            ),
            end=filter_datetime_format.format(
                op="lt",
                dt=str(end).replace(" ", "T"),
            ),
        )
    ).json()

    # Get all event items for each event
    item_request_format = (
        LEGISTAR_EVENT_BASE
        + "/{event_id}/EventItems?AgendaNote=1&MinutesNote=1&Attachments=1"
    )
    for event in response:
        # Attach the Event Items to the event
        event["EventItems"] = requests.get(
            item_request_format.format(client=client, event_id=event["EventId"])
        ).json()

        # Attach info for the body responsible for this event
        event[LEGISTAR_EV_BODY] = get_legistar_body(
            client=client, body_id=event["EventBodyId"], use_cache=True
        )

        # Get vote information
        for event_item in event["EventItems"]:
            vote_request_format = LEGISTAR_VOTE_BASE + "/{event_item_id}/Votes"
            event_item["EventItemVoteInfo"] = requests.get(
                vote_request_format.format(
                    client=client,
                    event_item_id=event_item["EventItemId"],
                )
            ).json()

            # Get person information
            for vote_info in event_item["EventItemVoteInfo"]:
                vote_info["PersonInfo"] = get_legistar_person(
                    client=client,
                    person_id=vote_info["VotePersonId"],
                    use_cache=True,
                )

            if (
                not isinstance(event_item["EventItemMatterId"], int)
                or event_item["EventItemMatterId"] < 0
            ):
                event_item[LEGISTAR_MATTER_SPONSORS] = None
            else:
                # this matter's sponsors
                sponsor_request_format = (
                    LEGISTAR_MATTER_BASE + "/{event_item_matter_id}/Sponsors"
                )
                sponsors = requests.get(
                    sponsor_request_format.format(
                        client=client,
                        event_item_matter_id=event_item["EventItemMatterId"],
                    )
                ).json()

                # legistar MatterSponsor just has a reference to a Person
                # so further obtain the actual Person information
                for sponsor in sponsors:
                    sponsor[LEGISTAR_SPONSOR_PERSON] = get_legistar_person(
                        client=client,
                        person_id=sponsor["MatterSponsorNameId"],
                        use_cache=True,
                    )

                event_item[LEGISTAR_MATTER_SPONSORS] = sponsors

    log.debug(f"Collected {len(response)} Legistar events")
    return response


class ContentUriScrapeResult(NamedTuple):
    class Status(enum.IntEnum):
        # Web page(s) are in unrecognized structure
        UnrecognizedPatternError = -1
        # Error in accessing some resource
        ResourceAccessError = -2
        # Video was not provided for the event
        ContentNotProvidedError = -3
        # Found URIs to video and optional caption
        Ok = 0

    status: Status
    uris: Optional[List[ContentURIs]] = None


def get_legistar_content_uris(client: str, legistar_ev: Dict) -> ContentUriScrapeResult:
    """
    Return URLs for videos and captions from a Legistar/Granicus-hosted video web page

    Parameters
    ----------
    client: str
        Which legistar client to target. Ex: "seattle"
    legistar_ev: Dict
        Data for one Legistar Event.

    Returns
    -------
    ContentUriScrapeResult
        status: ContentUriScrapeResult.Status
            Status code describing the scraping process. Use uris only if status is Ok
        uris: Optional[List[ContentURIs]]
            URIs for video and optional caption

    Raises
    ------
    NotImplementedError
        Means the content structure of the web page hosting session video has changed.
        We need explicit review and update the scraping code.

    See Also
    --------
    LegistarScraper.get_content_uris()
    cdp_scrapers.legistar_content_parsers
    """
    global video_page_parser

    # prefer video file path in legistar Event.EventVideoPath
    if legistar_ev[LEGISTAR_SESSION_VIDEO_URI]:
        return (
            ContentUriScrapeResult.Status.Ok,
            [
                ContentURIs(
                    video_uri=str_simplified(legistar_ev[LEGISTAR_SESSION_VIDEO_URI]),
                    caption_uri=None,
                )
            ],
        )
    if not legistar_ev[LEGISTAR_EV_SITE_URL]:
        return (ContentUriScrapeResult.Status.UnrecognizedPatternError, None)

    try:
        # a td tag with a certain id pattern.
        # this is usually something like
        # https://somewhere.legistar.com/MeetingDetail.aspx...
        # that is a summary-like page for a meeting
        with urlopen(legistar_ev[LEGISTAR_EV_SITE_URL]) as resp:
            soup = BeautifulSoup(resp.read(), "html.parser")

    except (URLError, HTTPError) as e:
        log.debug(f"{legistar_ev[LEGISTAR_EV_SITE_URL]}: {str(e)}")
        return (ContentUriScrapeResult.Status.ResourceAccessError, None)

    # this gets us the url for the web PAGE containing the video
    # video link is provided in the window.open()command inside onclick event
    # <a id="ctl00_ContentPlaceHolder1_hypVideo"
    # data-event-id="75f1e143-6756-496f-911b-d3abe61d64a5"
    # data-running-text="In&amp;nbsp;progress" class="videolink"
    # onclick="window.open('Video.aspx?
    # Mode=Granicus&amp;ID1=8844&amp;G=D64&amp;Mode2=Video','video');
    # return false;"
    # href="#" style="color:Blue;font-family:Tahoma;font-size:10pt;">Video</a>
    extract_url = soup.find(
        "a",
        id=re.compile(r"ct\S*_ContentPlaceHolder\S*_hypVideo"),
        class_="videolink",
    )
    if extract_url is None:
        return (ContentUriScrapeResult.Status.UnrecognizedPatternError, None)
    # the <a> tag will not have this attribute if there is no video
    if "onclick" not in extract_url.attrs:
        return (ContentUriScrapeResult.Status.ContentNotProvidedError, None)

    # NOTE: after this point, failing to scrape video url should raise an exception.
    # we need to be alerted that we probabaly have a new web page structure.

    extract_url = extract_url["onclick"]
    start = extract_url.find("'") + len("'")
    end = extract_url.find("',")
    video_page_url = f"https://{client}.legistar.com/{extract_url[start:end]}"

    log.debug(f"{legistar_ev[LEGISTAR_EV_SITE_URL]} -> {video_page_url}")

    try:
        with urlopen(video_page_url) as resp:
            # now load the page to get the actual video url
            soup = BeautifulSoup(resp.read(), "html.parser")

            if client in video_page_parser:
                # we alrady know which format parser to call
                uris = video_page_parser[client](client, soup)
            else:
                for parser in all_parsers:
                    uris = parser(client, soup)
                    if uris is not None:
                        # remember so we just call this from here on
                        video_page_parser[client] = parser
                        log.debug(f"{parser} for {client}")
                        break
                else:
                    uris = None
    except HTTPError as e:
        log.debug(f"Error opening {video_page_url}:\n{str(e)}")
        return (ContentUriScrapeResult.Status.ResourceAccessError, None)

    if uris is None:
        raise NotImplementedError(
            "get_legistar_content_uris() needs attention. "
            f"Unrecognized video web page HTML structure: {video_page_url}"
        )
    return (ContentUriScrapeResult.Status.Ok, uris)

