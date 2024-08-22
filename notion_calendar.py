from flask import Flask, Response, request
from flask_httpauth import HTTPTokenAuth
from ics import Calendar, Event
from retry import retry
from werkzeug.exceptions import Unauthorized
import arrow
import cachetools.func
import json
import logging
import os
import requests
import typing
from ics.grammar.parse import ContentLine
from ics.serializers.icalendar_serializer import CalendarSerializer
from ics.serializers.event_serializer import EventSerializer
from dateutil import tz

app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')

app.config['NOTION_API_KEY'] = os.environ['NOTION_API_KEY']
app.config['NOTION_API_VERSION'] = '2022-06-28'
app.config['TOKENS'] = json.loads(os.environ['TOKENS'])
ljubljana = tz.gettz('Europe/Ljubljana')

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
waitress_logger = logging.getLogger('waitress')
waitress_logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CustomCalendarSerializer(CalendarSerializer):
    """
    Serialize a calendar with some additional properties
    """
    def serialize_2url(calendar, container):
        if hasattr(calendar, 'url') and calendar.url:
            container.append(ContentLine("URL", value=""))

    def serialize_3name(calendar, container):
        if hasattr(calendar, 'name') and calendar.name:
            container.append(ContentLine("NAME", value=calendar.name))

    def serialize_3description(calendar, container):
        if hasattr(calendar, 'description') and calendar.description:
            container.append(ContentLine("DESCRIPTION", value=calendar.description))

    def serialize_3timezone_id(calendar, container):
        container.append(ContentLine("TIMEZONE-ID", value="Europe/Ljubljana"))

    def serialize_3color(calendar, container):
        container.append(ContentLine("COLOR", value="136:115:80"))


class CustomEventSerializer(EventSerializer):
    """
    Serialize event with additional properties
    """
    def serialize_color(event, container):
        if hasattr(event, 'color') and event.color:
            container.append(ContentLine("COLOR", value=event.color))


@auth.verify_token
def verify_token(token: str) -> str:
    """
    Verify that the bearer token sent is in the list of our tokens (and return the related username
    """
    if token in app.config['TOKENS']:
        return app.config['TOKENS'][token]


def validate_token(token: typing.Optional[str]):
    """
    Rudimentary implementation of bearer tokens over query string
    """
    if token is None or token == "":
        raise Unauthorized()
    res = verify_token(token)
    if res is None or res == "":
        raise Unauthorized()


@retry(Exception, tries=5, delay=1)
def get_page(url: str, page_count: int, body: typing.Optional[typing.Dict] = None, headers: typing.Optional[typing.Dict] = None) -> typing.Optional[typing.Dict]:
    logger.info(f'Fetching {url}, page {page_count}')
    query_response = requests.post(url, json=body, headers=headers)
    query_response.raise_for_status()
    return query_response.json()


@retry(Exception, tries=5, delay=0)
@cachetools.func.ttl_cache(maxsize=128, ttl=600)
def get_events(notion_database_id: str) -> typing.Dict:
    """
    Fetch all data and metadata from a Notion database. Paginate if multiple pages exist.
    Cache the result for 10 minutes
    :param notion_database_id: The ID of a Notion database. I.e. the part after your workspace. Specifically, Notion
      URLs will look something like this: https://www.notion.so/<workspace>/<database_id>
    :return: A dictionary containing data ("results") and metadata ("info")
    """
    notion_api_key = app.config['NOTION_API_KEY']
    notion_api_version = app.config['NOTION_API_VERSION']
    database_info = f'https://api.notion.com/v1/databases/{notion_database_id}'
    database_query = f'https://api.notion.com/v1/databases/{notion_database_id}/query'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Notion-Version': f'{notion_api_version}',
    }

    params = {}
    pages_and_databases = []

    logger.info(f'Fetching {database_info}')
    info_response = requests.get(database_info, json=params, headers=headers)
    info_response.raise_for_status()

    page_count = 1
    while True:
        search_response_obj = get_page(database_query, page_count, params, headers)
        pages_and_databases.extend(search_response_obj.get("results"))
        if not search_response_obj.get("has_more"):
            break
        page_count += 1
        params["start_cursor"] = search_response_obj.get("next_cursor")

    return {
        'info': info_response.json(),
        'results': pages_and_databases
    }


@cachetools.func.ttl_cache(maxsize=128, ttl=60)
def get_calendar(notion_database_id: str) -> str:
    """
    Fetch a database from Notion and create a ICS-compatible calendar as string.
    :param notion_database_id: The Notion database ID
    :return: The ICS file
    """
    events = get_events(notion_database_id)
    logger.info(f'Parsing results for {notion_database_id}...')
    info = events["info"]
    results = events["results"]

    c = Calendar()
    c.Meta.serializer = CustomCalendarSerializer
    c.name = info["title"][0]["plain_text"]
    if info["description"] is not None and len(info["description"]) > 0:
        calendar.description = info["description"][0]["plain_text"].strip()

    for obj in results:
        # Skip archived and trashed objects

        # Fetch a few properties into variables
        properties = obj["properties"]
        title_prop = properties[""]
        location_prop = properties["Location"]
        type_prop = properties["Type"]
        tags_prop = properties["Tags"]
        date_prop = properties["Date"]
        page_prop = properties["Page"]

        try:
            if title_prop is None:
                continue
        except IndexError:
            continue

        if len(title_prop["title"]) == 0:
            continue

        title_0 = title_prop["title"][0]

        if obj["archived"] or obj["in_trash"]:
            continue

        if date_prop["date"] is None:
            continue

        if date_prop["date"]["start"] is None:
            continue

        try:
            if date_prop is None:
                continue
        except IndexError:
            continue

        try:
            if location_prop is None:
                pass
        except IndexError:
            location_prop = None

        e = Event()
        e.Meta.serializer = CustomEventSerializer
        if type_prop["select"] is not None and type_prop["select"]["name"] is not None:
            e.name = "[" + type_prop["select"]["name"].strip() + "] "
            e.categories = [
                type_prop["select"]["name"].strip()
            ]
            if type_prop["select"]["color"]:
                e.color = type_prop["select"]["color"]
        else:
            e.name = ""
            e.categories = []

        if obj["icon"] is not None and "emoji" in obj["icon"] and obj["icon"]["emoji"] is not None:
            e.name = e.name + obj["icon"]["emoji"] + " "

        e.name = e.name + title_0["plain_text"].strip()

        for tag in tags_prop["multi_select"]:
            e.categories = e.categories + [tag["name"]]

        e.uid = obj["id"]
        e.url = obj["url"]
        if date_prop["date"] is not None:
            is_all_day = False
            if date_prop["date"]["start"] is not None:
                try:
                    if len(date_prop["date"]["start"]) == 10:
                        is_all_day = True
                        e.begin = arrow.get(date_prop["date"]["start"]).floor('day')
                    else:
                        e.begin = arrow.get(date_prop["date"]["start"], tzinfo=ljubljana)
                except arrow.ParserError:
                    e.begin = arrow.get(date_prop["date"]["start"])
                if is_all_day:
                    e.make_all_day()

            if date_prop["date"]["end"] is not None:
                # End days, by iCal definition are EXCLUSIVE. So if the event is a whole day event,
                # we must define it as ending one day later.
                if len(date_prop["date"]["end"]) == 10:
                    e.end = arrow.get(date_prop["date"]["end"]).floor('day').shift(days=+1)
                else:
                    try:
                        e.end = arrow.get(date_prop["date"]["end"], tzinfo=ljubljana)
                    except arrow.ParserError:
                        e.end = arrow.get(date_prop["date"]["end"])
            else:
                if is_all_day:
                    e.end = e.begin.shift(days=+1)
                else:
                    e.end = e.begin

        try:
            if page_prop is not None and page_prop["url"] is not None:
                e.description = page_prop["url"] + '\r\n'
            else:
                e.description = ""
        except TypeError:
            e.description = ""

        if properties["Status"] and properties["Status"]["status"] and properties["Status"]["status"]["name"]:
            status = properties["Status"]["status"]["name"]
            if status == "Not going":
                e.status = "CANCELLED"
            elif status == "Confirmed":
                e.status = "CONFIRMED"
            elif status == "Need more info":
                e.status = "TENTATIVE"

        # TENTATIVE, CONFIRMED, CANCELLED
        # e.status =
        if obj["created_time"] is not None:
            e.created = arrow.get(obj["created_time"])
        if obj["last_edited_time"] is not None:
            e.last_modified = arrow.get(obj["last_edited_time"])
        if location_prop is not None and len(location_prop["rich_text"]) > 0:
            e.location = location_prop["rich_text"][0]["plain_text"].strip()
        c.events.add(e)

    logger.info(f'Done for {notion_database_id}...')
    result = ''.join(c.serialize_iter())

    return result


#                                                     notion_view_id = vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
# https://www.notion.so/<workspace>/f0c46e3a76cc4536ae84b0e1ffa58a72?v=761088f2880549f79b458d19e5333d54
#             notion_database_id =  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
@app.route("/calendar/bearer/<notion_database_id>")
@auth.login_required
def calendar(notion_database_id: str):
    """
    Get a calendar, authenticating via Bearer token
    :param notion_database_id: The ID of a Notion database, as retrieved from Notion URL. Ensure that the integration
      service has access to this database.
    :return: The ICS calendar file
    """
    return Response(get_calendar(notion_database_id), mimetype='text/calendar')


@app.route("/calendar/qs/<notion_database_id>")
def calendar_qs(notion_database_id: str):
    """
    Get the calendar, authenticating via token in URL
    :param notion_database_id: The ID of a Notion database, as retrieved from Notion URL. Ensure that the integration
      service has access to this database.
    :return: The ICS calendar file
    """
    validate_token(request.args.get('token'))
    return Response(get_calendar(notion_database_id), mimetype='text/calendar')


def create_app() -> Flask:
    """
    Called from waitress server to start the app
    :return: Flask app. Nothing special here.
    """
    return app


# Starts flask in development mode if calling via "python <name>"
if __name__ == '__main__':
    app.run(debug=True, port=8080, host="0.0.0.0")
