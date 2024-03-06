# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, Optional

import pendulum
import requests
from airbyte_cdk.sources.declarative.auth.token import BasicHttpAuthenticator
from airbyte_cdk.sources.declarative.extractors import DpathExtractor
from airbyte_cdk.sources.declarative.extractors.record_filter import RecordFilter
from airbyte_cdk.sources.declarative.requesters.http_requester import HttpRequester
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_options_provider import (
    InterpolatedRequestOptionsProvider,
    RequestInput,
)
from airbyte_cdk.sources.declarative.types import StreamSlice, StreamState
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_protocol.models import FailureType


@dataclass
class MailChimpRequester(HttpRequester):
    """
    Introduce `get_data_center_location` method to define data_center based on Authenticator type and update config on the fly.
    """

    request_body_json: Optional[RequestInput] = None
    request_headers: Optional[RequestInput] = None
    request_parameters: Optional[RequestInput] = None
    request_body_data: Optional[RequestInput] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:

        self.request_options_provider = InterpolatedRequestOptionsProvider(
            request_body_data=self.request_body_data,
            request_body_json=self.request_body_json,
            request_headers=self.request_headers,
            request_parameters=self.request_parameters,
            config=self.config,
            parameters=parameters or {},
        )
        super().__post_init__(parameters)

    def get_url_base(self) -> str:
        self.get_data_center_location()
        return super().get_url_base()

    def get_data_center_location(self):
        if not self.config.get("data_center"):
            if isinstance(self.authenticator, BasicHttpAuthenticator):
                data_center = self.config["credentials"]["apikey"].split("-").pop()
            else:
                data_center = self.get_oauth_data_center(self.config["credentials"]["access_token"])
            self.config["data_center"] = data_center

    @staticmethod
    def get_oauth_data_center(access_token: str) -> str:
        """
        Every Mailchimp API request must be sent to a specific data center.
        The data center is already embedded in API keys, but not OAuth access tokens.
        This method retrieves the data center for OAuth credentials.
        """
        response = requests.get("https://login.mailchimp.com/oauth2/metadata", headers={"Authorization": "OAuth {}".format(access_token)})

        # Requests to this endpoint will return a 200 status code even if the access token is invalid.
        error = response.json().get("error")
        if error == "invalid_token":
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                internal_message=error,
                message="The access token you provided was invalid. Please check your credentials and try again.",
            )
        return response.json()["dc"]


class MailChimpRecordFilter(RecordFilter):
    """
    Filter applied on a list of Records.
    """

    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.parameters = parameters

    def filter_records(
        self,
        records: List[Mapping[str, Any]],
        stream_state: StreamState,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> List[Mapping[str, Any]]:
        current_state = [x for x in stream_state.get("states", []) if x["partition"]["id"] == stream_slice.partition["id"]]
        # TODO: REF what to do if no start_date mentioned (see manifest)
        #  implement the same logic
        cursor_value = self.get_filter_date(self.config.get("start_date"), current_state)
        if cursor_value:
            return [record for record in records if record[self.parameters["cursor_field"]] > cursor_value]
        return records

    def get_filter_date(self, start_date: str, state_value: list) -> str:
        """
        Calculate the filter date to pass in the request parameters by comparing the start_date
        with the value of state obtained from the stream_slice.
        If only one value exists, use it by default. Otherwise, return None.
        If no filter_date is provided, the API will fetch all available records.
        """

        start_date_parsed = pendulum.parse(start_date).to_iso8601_string() if start_date else None
        state_date_parsed = (
            pendulum.parse(state_value[0]["cursor"][self.parameters["cursor_field"]]).to_iso8601_string() if state_value else None
        )

        # Return the max of the two dates if both are present. Otherwise return whichever is present, or None.
        if start_date_parsed or state_date_parsed:
            return max(filter(None, [start_date_parsed, state_date_parsed]), default=None)


class MailChimpRecordExtractorEmailActivity(DpathExtractor):
    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        records = super().extract_records(response=response)
        return [{**record, **activity_item} for record in records for activity_item in record.pop("activity", [])]
