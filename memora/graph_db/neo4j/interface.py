import logging
from typing import Optional

import neo4j
from neo4j import AsyncGraphDatabase
from typing_extensions import override

from ...vector_db.base import BaseVectorDB
from .agent import Neo4jAgent
from .interaction import Neo4jInteraction
from .memory import Neo4jMemory
from .organization import Neo4jOrganization
from .user import Neo4jUser


class Neo4jGraphInterface(
    Neo4jOrganization, Neo4jAgent, Neo4jUser, Neo4jInteraction, Neo4jMemory
):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str,
        associated_vector_db: Optional[BaseVectorDB] = None,
        enable_logging: bool = False,
    ):
        """
        A unified interface for interacting with the Neo4j graph database.

        Args:
            uri (str): The URI of the Neo4j database.
            username (str): The username for authentication.
            password (str): The password for authentication.
            database (str): The name of the Neo4j database.
            associated_vector_db (Optional[BaseVectorDB]): The vector database to be associated with the graph for data consistency.
            enable_logging (bool): Whether to enable console logging
        """
        self.driver = AsyncGraphDatabase.driver(uri=uri, auth=(username, password))
        self.database = database
        self.associated_vector_db = associated_vector_db

        # Configure logging
        self.logger = logging.getLogger(__name__)
        if enable_logging:
            logging.basicConfig(level=logging.INFO)

    @override
    def get_associated_vector_db(self) -> Optional[BaseVectorDB]:
        return self.associated_vector_db

    @override
    async def close(self):
        self.logger.info("Closing Neo4j driver")
        await self.driver.close()

    @override
    async def setup(self, *args, **kwargs) -> None:
        """Sets up Neo4j database constraints and indices for the graph schema."""
        async def create_constraints_and_indexes(tx):
            self.logger.info("Creating constraints and indexes")
            # Organization node key
            await tx.run(
                """
                CREATE CONSTRAINT unique_org_id IF NOT EXISTS 
                FOR (o:Org) REQUIRE o.org_id IS NODE KEY
                """
            )

            # User node key
            await tx.run(
                """
                CREATE CONSTRAINT unique_org_user IF NOT EXISTS
                FOR (u:User) REQUIRE (u.org_id, u.user_id) IS NODE KEY
                """
            )

            # Agent node key
            await tx.run(
                """
                CREATE CONSTRAINT unique_org_agent IF NOT EXISTS 
                FOR (a:Agent) REQUIRE (a.org_id, a.agent_id) IS NODE KEY
                """
            )

            # Memory node key
            await tx.run(
                """
                CREATE CONSTRAINT unique_user_memory IF NOT EXISTS
                FOR (m:Memory) REQUIRE (m.org_id, m.user_id, m.memory_id) IS NODE KEY
                """
            )

            # Interaction node key
            await tx.run(
                """
                CREATE CONSTRAINT unique_user_interaction IF NOT EXISTS
                FOR (i:Interaction) REQUIRE (i.org_id, i.user_id, i.interaction_id) IS NODE KEY
                """
            )

            # Date node key
            await tx.run(
                """
                CREATE CONSTRAINT unique_user_date IF NOT EXISTS
                FOR (d:Date) REQUIRE (d.org_id, d.user_id, d.date) IS NODE KEY
                """
            )

        self.logger.info("Setting up Neo4j database constraints and indices")
        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(create_constraints_and_indexes)
        self.logger.info("Setup complete")
