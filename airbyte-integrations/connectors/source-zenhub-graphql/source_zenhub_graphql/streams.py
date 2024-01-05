from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
#from airbyte_cdk.sources.streams.http import HttpStream as CDK_Stream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from sgqlc.endpoint.http import HTTPEndpoint 
from sgqlc.operation import Operation
from sgqlc.types import Type, Field, list_of

from .graphql import (
    Viewer
    , Query
    , WorkspaceConnection
    , Workspace as WSGraphql
    , Repository
    , RepositoryConnection
    , Pipeline
    , PipelineConnection
    , PipelineIssue
    , Issue
    , IssuesConnection
    , Priority
)

# Basic full refresh stream
#class ZenhubGraphqlStream(HttpStream, ABC):
class ZenhubGraphqlStream(HTTPEndpoint, ABC):

    #url_base = "https://api.zenhub.com/public/graphql"

    """
    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class ZenhubGraphqlStream(HttpStream, ABC)` which is the current class
    `class Customers(ZenhubGraphqlStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(ZenhubGraphqlStream)` contains behavior to pull data for employees using v1/employees
    """
    def __init__(self,api_key: str, url_base: str="https://api.zenhub.com/public/graphql", **kwargs):
        self.headers = {'Authorization': f'Bearer {api_key}'}
        super().__init__(url_base, self.headers, **kwargs) 
     
    def execute_query(self, query):
        return self(query)
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
       
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}


class ZenhubWorkspace(ZenhubGraphqlStream):

    def __init__(self, api_key, workspace_name):
        super().__init__(api_key)
        self.workspace_name = workspace_name
    
    def get_ws_query(self):
        ws_op = Operation(Query)
        viewer_query = ws_op.viewer
        viewer_query.id()
        ws_op_query = viewer_query.searchWorkspaces(query=self.workspace_name)
        workspace_node = ws_op_query.nodes.__as__(WSGraphql)  # Use the Workspace type
        workspace_node.id()
        workspace_node.name()
        repository_node = workspace_node.repositoriesConnection.nodes.__as__(Repository)
        repository_node.id()
        repository_node.name()

        return ws_op


    def fetch_data(self):
        query = self.get_ws_query()
        try: 
            response = self.execute_query(query)
            return response, 200
        except Exception as e:
            return {"error": str(e)}, 500
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        pass

    def primary_key(self):
        pass
    
        


# Basic incremental stream
class IncrementalZenhubGraphqlStream(ZenhubGraphqlStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Employees(IncrementalZenhubGraphqlStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")