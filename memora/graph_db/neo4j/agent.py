from typing import List, Optional
import neo4j
import neo4j.exceptions
import shortuuid
from typing_extensions import override
from memora.schema import models
from ..base import BaseGraphDB

class Neo4jAgent(BaseGraphDB):
    @override
    async def create_agent(
        self, org_id: str, agent_label: str, user_id: Optional[str] = None
    ) -> models.Agent:
        if not all(param and isinstance(param, str) for param in (org_id, agent_label)):
            raise ValueError(
                "Both `org_id` and `agent_label` must be a string and have a value."
            )
        
        agent_id = shortuuid.uuid()
        self.logger.info(f"Creating new agent with ID {agent_id}")

        async def create_agent_tx(tx):
            if user_id:
                result = await tx.run(
                    """
                    MATCH (o:Org {org_id: $org_id}), (u:User {org_id: $org_id, user_id: $user_id})
                    CREATE (a:Agent {
                        org_id: $org_id,
                        user_id: $user_id,
                        agent_id: $agent_id,
                        agent_label: $agent_label,
                        created_at: datetime()
                    })
                    CREATE (o)-[:HAS_AGENT]->(a)
                    CREATE (u)-[:HAS_AGENT]->(a)
                    RETURN a{.org_id, .user_id, .agent_id, .agent_label, .created_at} as agent
                    """,
                    org_id=org_id,
                    user_id=user_id,
                    agent_id=agent_id,
                    agent_label=agent_label,
                )
            else:
                result = await tx.run(
                    """
                    MATCH (o:Org {org_id: $org_id})
                    CREATE (a:Agent {
                        org_id: $org_id,
                        agent_id: $agent_id,
                        agent_label: $agent_label,
                        created_at: datetime()
                    })
                    CREATE (o)-[:HAS_AGENT]->(a)
                    RETURN a{.org_id, .agent_id, .agent_label, .created_at} as agent
                    """,
                    org_id=org_id,
                    agent_id=agent_id,
                    agent_label=agent_label,
                )
            
            record = await result.single()
            return record["agent"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            agent_data = await session.execute_write(create_agent_tx)
            
            if agent_data is None:
                raise neo4j.exceptions.Neo4jError("Failed to create agent.")
            
            return models.Agent(
                org_id=agent_data["org_id"],
                agent_id=agent_data["agent_id"],
                user_id=agent_data.get("user_id"),
                agent_label=agent_data["agent_label"],
                created_at=(agent_data["created_at"]).to_native(),
            )

    @override
    async def update_agent(
        self, org_id: str, agent_id: str, new_agent_label: str
    ) -> models.Agent:
        async def update_agent_tx(tx):
            result = await tx.run(
                """
                MATCH (a:Agent {org_id: $org_id, agent_id: $agent_id})
                SET a.agent_label = $new_agent_label
                RETURN a{.org_id, .user_id, .agent_id, .agent_label, .created_at} as agent
                """,
                org_id=org_id,
                agent_id=agent_id,
                new_agent_label=new_agent_label,
            )
            record = await result.single()
            return record["agent"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            agent_data = await session.execute_write(update_agent_tx)
            if agent_data is None:
                raise neo4j.exceptions.Neo4jError("Agent not found")
            return models.Agent(
                org_id=agent_data["org_id"],
                agent_id=agent_data["agent_id"],
                user_id=agent_data.get("user_id"),
                agent_label=agent_data["agent_label"],
                created_at=(agent_data["created_at"]).to_native(),
            )

    @override
    async def delete_agent(self, org_id: str, agent_id: str) -> None:
        async def delete_agent_tx(tx):
            await tx.run(
                """
                MATCH (a:Agent {org_id: $org_id, agent_id: $agent_id})
                DETACH DELETE a
                """,
                org_id=org_id,
                agent_id=agent_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(delete_agent_tx)

    @override
    async def get_agent(self, org_id: str, agent_id: str) -> models.Agent:
        async def get_agent_tx(tx):
            result = await tx.run(
                """
                MATCH (a:Agent {org_id: $org_id, agent_id: $agent_id})
                RETURN a{.org_id, .user_id, .agent_id, .agent_label, .created_at} as agent
                """,
                org_id=org_id,
                agent_id=agent_id,
            )
            record = await result.single()
            return record["agent"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            agent_data = await session.execute_read(get_agent_tx)
            if agent_data is None:
                raise neo4j.exceptions.Neo4jError("Agent not found")
            return models.Agent(
                org_id=agent_data["org_id"],
                agent_id=agent_data["agent_id"],
                user_id=agent_data.get("user_id"),
                agent_label=agent_data["agent_label"],
                created_at=(agent_data["created_at"]).to_native(),
            )

    @override
    async def get_all_org_agents(self, org_id: str) -> List[models.Agent]:
        async def get_org_agents_tx(tx):
            result = await tx.run(
                """
                MATCH (o:Org {org_id: $org_id})-[:HAS_AGENT]->(a:Agent)
                RETURN a{.org_id, .user_id, .agent_id, .agent_label, .created_at} as agent
                """,
                org_id=org_id,
            )
            records = await result.value("agent", [])
            return records

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            agents_data = await session.execute_read(get_org_agents_tx)
            return [
                models.Agent(
                    org_id=agent_data["org_id"],
                    agent_id=agent_data["agent_id"],
                    user_id=agent_data.get("user_id"),
                    agent_label=agent_data["agent_label"],
                    created_at=(agent_data["created_at"]).to_native(),
                )
                for agent_data in agents_data
            ]

    @override
    async def get_all_user_agents(
        self, org_id: str, user_id: str
    ) -> List[models.Agent]:
        async def get_user_agents_tx(tx):
            result = await tx.run(
                """
                MATCH (u:User {org_id: $org_id, user_id: $user_id})-[:HAS_AGENT]->(a:Agent)
                RETURN a{.org_id, .user_id, .agent_id, .agent_label, .created_at} as agent
                """,
                org_id=org_id,
                user_id=user_id,
            )
            records = await result.value("agent", [])
            return records

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            agents_data = await session.execute_read(get_user_agents_tx)
            return [
                models.Agent(
                    org_id=agent_data["org_id"],
                    agent_id=agent_data["agent_id"],
                    user_id=agent_data.get("user_id"),
                    agent_label=agent_data["agent_label"],
                    created_at=(agent_data["created_at"]).to_native(),
                )
                for agent_data in agents_data
            ]