import re
import time
from collections.abc import Callable
from collections.abc import Generator
from functools import wraps
from typing import Any
from typing import cast

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web import SlackResponse

from danswer.utils.logger import setup_logger

logger = setup_logger()

# number of messages we request per page when fetching paginated slack messages
_SLACK_LIMIT = 900


def get_message_link(
    event: dict[str, Any], workspace: str, channel_id: str | None = None
) -> str:
    channel_id = channel_id or cast(
        str, event["channel"]
    )  # channel must either be present in the event or passed in
    message_ts = cast(str, event["ts"])
    message_ts_without_dot = message_ts.replace(".", "")
    thread_ts = cast(str | None, event.get("thread_ts"))
    return (
        f"https://{workspace}.slack.com/archives/{channel_id}/p{message_ts_without_dot}"
        + (f"?thread_ts={thread_ts}" if thread_ts else "")
    )


def make_slack_api_call_logged(
    call: Callable[..., SlackResponse],
) -> Callable[..., SlackResponse]:
    @wraps(call)
    def logged_call(**kwargs: Any) -> SlackResponse:
        logger.debug(f"Making call to Slack API '{call.__name__}' with args '{kwargs}'")
        result = call(**kwargs)
        logger.debug(f"Call to Slack API '{call.__name__}' returned '{result}'")
        return result

    return logged_call


def make_slack_api_call_paginated(
    call: Callable[..., SlackResponse],
) -> Callable[..., Generator[dict[str, Any], None, None]]:
    """Wraps calls to slack API so that they automatically handle pagination"""

    @wraps(call)
    def paginated_call(**kwargs: Any) -> Generator[dict[str, Any], None, None]:
        cursor: str | None = None
        has_more = True
        while has_more:
            response = call(cursor=cursor, limit=_SLACK_LIMIT, **kwargs)
            yield cast(dict[str, Any], response.validate())
            cursor = cast(dict[str, Any], response.get("response_metadata", {})).get(
                "next_cursor", ""
            )
            has_more = bool(cursor)

    return paginated_call


def make_slack_api_rate_limited(
    call: Callable[..., SlackResponse], max_retries: int = 3
) -> Callable[..., SlackResponse]:
    """Wraps calls to slack API so that they automatically handle rate limiting"""

    @wraps(call)
    def rate_limited_call(**kwargs: Any) -> SlackResponse:
        for _ in range(max_retries):
            try:
                # Make the API call
                response = call(**kwargs)

                # Check for errors in the response, will raise `SlackApiError`
                # if anything went wrong
                response.validate()
                return response

            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    # Handle rate limiting: get the 'Retry-After' header value and sleep for that duration
                    retry_after = int(e.response.headers.get("Retry-After", 1))
                    logger.info(
                        f"Slack call rate limited, retrying after {retry_after} seconds. Exception: {e}"
                    )
                    time.sleep(retry_after)
                if e.response["error"] in ["already_reacted", "no_reaction"]:
                    # The response isn't used for reactions, this is basically just a pass
                    return e.response
                else:
                    # Raise the error for non-transient errors
                    raise

        # If the code reaches this point, all retries have been exhausted
        raise Exception(f"Max retries ({max_retries}) exceeded")

    return rate_limited_call


class SlackTextCleaner:
    """Utility class to replace user IDs with usernames in a message.
    Handles caching, so the same request is not made multiple times
    for the same user ID"""

    def __init__(self, client: WebClient) -> None:
        self._client = client
        self._id_to_name_map: dict[str, str] = {}

    def _get_slack_name(self, user_id: str) -> str:
        if user_id not in self._id_to_name_map:
            try:
                response = make_slack_api_rate_limited(self._client.users_info)(
                    user=user_id
                )
                # prefer display name if set, since that is what is shown in Slack
                self._id_to_name_map[user_id] = (
                    response["user"]["profile"]["display_name"]
                    or response["user"]["profile"]["real_name"]
                )
            except SlackApiError as e:
                logger.exception(
                    f"Error fetching data for user {user_id}: {e.response['error']}"
                )
                raise

        return self._id_to_name_map[user_id]

    def _replace_user_ids_with_names(self, message: str) -> str:
        # Find user IDs in the message
        user_ids = re.findall("<@(.*?)>", message)

        # Iterate over each user ID found
        for user_id in user_ids:
            try:
                if user_id in self._id_to_name_map:
                    user_name = self._id_to_name_map[user_id]
                else:
                    user_name = self._get_slack_name(user_id)

                # Replace the user ID with the username in the message
                message = message.replace(f"<@{user_id}>", f"@{user_name}")
            except Exception:
                logger.exception(
                    f"Unable to replace user ID with username for user_id '{user_id}'"
                )

        return message

    def index_clean(self, message: str) -> str:
        """During indexing, replace pattern sets that may cause confusion to the model
        Some special patterns are left in as they can provide information
        ie. links that contain format text|link, both the text and the link may be informative
        """
        message = self._replace_user_ids_with_names(message)
        message = self.replace_tags_basic(message)
        message = self.replace_channels_basic(message)
        message = self.replace_special_mentions(message)
        message = self.replace_special_catchall(message)
        return message

    @staticmethod
    def replace_tags_basic(message: str) -> str:
        """Simply replaces all tags with `@<USER_ID>` in order to prevent us from
        tagging users in Slack when we don't want to"""
        # Find user IDs in the message
        user_ids = re.findall("<@(.*?)>", message)
        for user_id in user_ids:
            message = message.replace(f"<@{user_id}>", f"@{user_id}")
        return message

    @staticmethod
    def replace_channels_basic(message: str) -> str:
        """Simply replaces all channel mentions with `#<CHANNEL_ID>` in order
        to make a message work as part of a link"""
        # Find user IDs in the message
        channel_matches = re.findall(r"<#(.*?)\|(.*?)>", message)
        for channel_id, channel_name in channel_matches:
            message = message.replace(
                f"<#{channel_id}|{channel_name}>", f"#{channel_name}"
            )
        return message

    @staticmethod
    def replace_special_mentions(message: str) -> str:
        """Simply replaces @channel, @here, and @everyone so we don't tag
        a bunch of people in Slack when we don't want to"""
        # Find user IDs in the message
        message = message.replace("<!channel>", "@channel")
        message = message.replace("<!here>", "@here")
        message = message.replace("<!everyone>", "@everyone")
        return message

    @staticmethod
    def replace_links(message: str) -> str:
        """Replaces slack links e.g. `<URL>` -> `URL` and `<URL|DISPLAY>` -> `DISPLAY`"""
        # Find user IDs in the message
        possible_link_matches = re.findall(r"<(.*?)>", message)
        for possible_link in possible_link_matches:
            if not possible_link:
                continue
            # Special slack patterns that aren't for links
            if possible_link[0] not in ["#", "@", "!"]:
                link_display = (
                    possible_link
                    if "|" not in possible_link
                    else possible_link.split("|")[1]
                )
                message = message.replace(f"<{possible_link}>", link_display)
        return message

    @staticmethod
    def replace_special_catchall(message: str) -> str:
        """Replaces pattern of <!something|another-thing> with another-thing
        This is added for <!subteam^TEAM-ID|@team-name> but may match other cases as well
        """

        pattern = r"<!([^|]+)\|([^>]+)>"
        return re.sub(pattern, r"\2", message)

    @staticmethod
    def add_zero_width_whitespace_after_tag(message: str) -> str:
        """Add a 0 width whitespace after every @"""
        return message.replace("@", "@\u200B")
