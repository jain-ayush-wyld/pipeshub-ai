import asyncio
import re
import time
import os
import requests
import base64
import json
from tokenize import String
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from logging import Logger
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from click import Option
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from pydantic_core.core_schema import with_default_schema

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import (
    CollectionNames,
    Connectors,
    MimeTypes,
    OriginTypes,
)
from app.config.constants.http_status_code import HttpStatusCode
from app.connectors.core.base.connector.connector_service import BaseConnector
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.data_store.data_store import DataStoreProvider
from app.connectors.core.base.sync_point.sync_point import (
    SyncDataPointType,
    SyncPoint,
    generate_record_sync_point_key,
)
from app.connectors.core.registry.connector_builder import (
    AuthField,
    ConnectorBuilder,
    DocumentationLink,
)
from app.connectors.sources.github.common.apps import GithubApp
from app.models.entities import (
    AppRole,
    AppUser,
    FileRecord,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
    TicketRecord,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.sources.client.github.github import (
    GitHubClient,
    GitHubResponse,
    GitHubConfig,
)
from app.sources.external.github.github_ import GitHubDataSource
TOKEN = os.getenv("GITHUB_PAT")

@ConnectorBuilder("Github")\
    .in_group("Github")\
    .with_auth_type("API_TOKEN")\
    .with_description("Sync content from your Github instance")\
    .with_categories(["Knowledge Management"])\
    .configure(lambda builder: builder
        .with_icon("/assets/icons/connectors/bookstack.svg")\
        .add_documentation_link(DocumentationLink(
            "Github API Docs",
            "https://docs.github.com/en",
            "docs"
        ))
        .add_documentation_link(DocumentationLink(
            'Pipeshub Documentation',
            'https://docs.pipeshub.com/connectors/bookstack/bookstack',
            'pipeshub'
        ))
        .with_redirect_uri("", False)
        .add_auth_field(AuthField(
            name="token_id",
            display_name="Token ID",
            placeholder="YourTokenID",
            description="The Token ID generated from your Github profile",
            field_type="TEXT",
            max_length=100
        ))
    )\
    .build_decorator()
class GithubConnector(BaseConnector):
    """
    Connector for synching data from a Github instance.
    """
    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
    ) -> None:
        super().__init__(
            GithubApp(),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service
        )
        self.connector_name=Connectors.GITHUB
        
        self.data_source: Optional[GitHubDataSource] = None
        self.batch_size = 100
        self.max_concurrent_batches = 5
    async def init(self) -> bool:
        """_summary_

        Returns:
            bool: _description_
        """
        try:
            token_config = GitHubConfig(
                token =TOKEN
            )
            # Initialize client and datasource
            client = GitHubClient.build_with_config(token_config)
            # print(f"GitHub client created successfully: {client}")
        except Exception as e:
            print(f"Error: Failed to initialize GitHub client.")
            print(f"Details: {e}")
            return False
        self.data_source = GitHubDataSource(client)
        self.logger.info("Github client initialized successfully.")
        return True
    async def _get_issues_to_tickets(self):
        # response = await self.data_source.get_repo()
        auth_res = self.data_source.get_authenticated()
        # print("Authenticated User", auth_res)
        user_login = auth_res.data.login
        owner = user_login  # Use the same user as owner
        # Mention your repo name for testing
        # repo = "pipeshub-ai"  # Use this repository for testing, fork this repository to your account and give a star :D
        repo = "ayush_ign"
        # repo_res = await self.data_source.get_repo(owner, repo)
        issues_res = self.data_source.list_issues(owner, repo,state='all')
        # print(issues_res.data)
        only_issues=[]
        one_issue = None
        for issue in issues_res.data:
            if issue.pull_request is None:
                # print(issue.number)
                only_issues.append(issue)
                one_issue = issue
                # ticket_record = TicketRecord(
                # id=str(uuid.uuid4()),
                # record_name=one_issue.title,
                # record_type = RecordType.TICKET.value,
                # external_record_id=str(one_issue.id),
                # connector_name=self.connector_name,
                # origin=OriginTypes.CONNECTOR.value,
                # updated_at=int(one_issue.updated_at.astimezone(timezone.utc).timestamp()),
                # created_at= int(one_issue.created_at.astimezone(timezone.utc).timestamp()),
                # version = 0,
                # external_record_group_id="694ac840980fc35be585c035",
                # org_id="694ac7f8980fc35be585bfe8",
                # parent_record_type="FILE",
                # record_group_type="KB",
                # mime_type=MimeTypes.MARKDOWN,
                # weburl=one_issue.url
                # )
                # await self.data_entities_processor.on_new_records([(ticket_record,[])])
                break
        # creating issue as ticket
        ticket_record = TicketRecord(
            id=str(uuid.uuid4()),
            record_name=one_issue.title,
            record_type = RecordType.TICKET.value,
            external_record_id=str(one_issue.id),
            connector_name=self.connector_name,
            origin=OriginTypes.CONNECTOR.value,
            updated_at=int(one_issue.updated_at.astimezone(timezone.utc).timestamp()),
            created_at= int(one_issue.created_at.astimezone(timezone.utc).timestamp()),
            version = 0,
            external_record_group_id="694ac840980fc35be585c035",
            org_id="694ac7f8980fc35be585bfe8",
            parent_record_type="FILE",
            record_group_type="KB",
            mime_type=MimeTypes.MARKDOWN,
            weburl=one_issue.http_url
        )
        # t_issue = self.data_source.get_issue(owner,repo,6)
        # self.logger.info(t_issue.data.body)
        # issu=t_issue.data
        # self.logger.info(issu.body)
        # IMG_SRC_REGEX = r'<img[^>]+src=[\'"]([^\'"]+)[\'"]'
        # image_urls = re.findall(IMG_SRC_REGEX, issu.body)
        # image_url =image_urls[0]
        # img_bytes = await self.get_img_base64(image_url)
        # base64_image = base64.b64encode(img_bytes).decode("utf-8")
        # markdown_image = f"![Image](data:image/png;base64,{base64_image})"
        await self.data_entities_processor.on_new_records([(ticket_record,[])])

                
    async def get_img_bytes(self,image_url:String):
        # _inline_images_as_base64 of pr https://github.com/pipeshub-ai/pipeshub-ai/pull/952/files connector.py zammad code
        # for proper handling of all img. formats and errors raisen
        # get this tokn from users data for testing direct
        GITHUB_TOKEN=TOKEN
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json"
        }

        resp = requests.get(image_url, headers=headers,allow_redirects=True)
        # self.logger.info(type(resp))
        image_bytes = resp.content
        return image_bytes
    async def reindex_records(self):
        return
    async def run_incremental_sync(self):
        return
    async def run_sync(self):
        await self._get_issues_to_tickets()
        return
    async def stream_record(self,record:Record)->StreamingResponse:
        self.logger.info("🟣🟣🟣 STREAM_RECORD_MARKER 🟣🟣🟣")
        start_time = time.perf_counter()
        raw_url=record.weburl.split('/')
        self.logger.info(raw_url)
        repo_name=raw_url[4]
        username=raw_url[3]
        issue_number=int(raw_url[6])
        issue =  self.data_source.get_issue(owner=username,repo=repo_name,number=issue_number)
        # self.logger.info(type(issue))
        # self.logger.info(type(issue[0]))
        # self.logger.info(issue.data)
        # self.logger.info(issue.body)
        markdown_content:String =issue.data.body
        IMG_SRC_REGEX = r'<img[^>]+src=[\'"]([^\'"]+)[\'"]'
        img_url_list = re.findall(IMG_SRC_REGEX, markdown_content)
        for img_url in img_url_list:
            img_bytes= await self.get_img_bytes(img_url)
            base64_image = base64.b64encode(img_bytes).decode("utf-8")
            markdown_image = f"![Image](data:image/png;base64,{base64_image})"
            markdown_content=markdown_content + markdown_image
            self.logger.info(img_url)
            # break
        # self.logger.info(markdown_content)
        end_time = time.perf_counter()
        self.logger.info(f"Markdown size (chars): {len(markdown_content)}")
        elapsed_time = end_time - start_time
        self.logger.info(f"⏱️ Time taken for URL parsing and get_issue call: {elapsed_time:.4f} seconds")
        return StreamingResponse(
            markdown_content,
            media_type=record.mime_type if record.mime_type else "application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={record.record_name}"
            }
        )
       
    async def test_connection_and_access(self):
        return
    
    async def handle_webhook_notification(self, org_id: str, notification: Dict) -> bool:
        """Handle webhook notifications (optional - for real-time sync)."""
        try:
            # TODO: Implement webhook handling if supported
            return True
        except Exception as e:
            self.logger.error(f"Error handling webhook: {e}")
            return False
    def get_signed_url(self, record: Record) -> Optional[str]:
        """Get signed URL for record access (optional - if API supports it)."""
        # TODO: Implement if your API provides signed URLs
        return None
    async def cleanup(self) -> None:
        """
        Cleanup resources used by the connector.
        """
        self.logger.info("Cleaning up BookStack connector resources.")
        self.data_source = None   
    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService
    ) -> "BaseConnector":
        """
        Factory method to create a Github connector instance.

        Args:
            logger: Logger instance
            data_store_provider: Data store provider for database operations
            config_service: Configuration service for accessing credentials

        Returns:
            Initialized GithubConnector instance
        """
        data_entities_processor = DataSourceEntitiesProcessor(
            logger, data_store_provider, config_service
        )
        await data_entities_processor.initialize()

        return GithubConnector (
            logger, data_entities_processor, data_store_provider, config_service
        )
    

        