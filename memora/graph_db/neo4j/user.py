from typing import List
import neo4j
import neo4j.exceptions
import shortuuid
from typing_extensions import override
from memora.schema import models
from ..base import BaseGraphDB

class Neo4jUser(BaseGraphDB):
    @override
    async def create_user(self, org_id: str, user_name: str) -> models.User:
        if not all(param and isinstance(param, str) for param in (org_id, user_name)):
            raise ValueError(
                "Both `org_id` and `user_name` must be a string and have a value."
            )

        user_id = shortuuid.uuid()
        self.logger.info(f"Creating new user with ID {user_id}")

        async def create_user_tx(tx):
            result = await tx.run(
                """
                MATCH (o:Org {org_id: $org_id})
                CREATE (u:User {
                    org_id: $org_id,
                    user_id: $user_id,
                    user_name: $user_name,
                    created_at: datetime()
                })
                CREATE (u)-[:BELONGS_TO]->(o)
                CREATE (ic:InteractionCollection {
                    org_id: $org_id,
                    user_id: $user_id
                })
                CREATE (mc:MemoryCollection {
                    org_id: $org_id,
                    user_id: $user_id
                })
                CREATE (u)-[:INTERACTIONS_IN]->(ic)
                CREATE (u)-[:HAS_MEMORIES]->(mc)
                RETURN u{.org_id, .user_id, .user_name, .created_at} as user
                """,
                org_id=org_id,
                user_id=user_id,
                user_name=user_name,
            )
            record = await result.single()
            return record["user"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            user_data = await session.execute_write(create_user_tx)

            if user_data is None:
                raise neo4j.exceptions.Neo4jError("Failed to create user.")

            return models.User(
                org_id=user_data["org_id"],
                user_id=user_data["user_id"],
                user_name=user_data["user_name"],
                created_at=(user_data["created_at"]).to_native(),
            )

    @override
    async def update_user(
        self, org_id: str, user_id: str, new_user_name: str
    ) -> models.User:
        async def update_user_tx(tx):
            result = await tx.run(
                """
                MATCH (u:User {org_id: $org_id, user_id: $user_id})
                SET u.user_name = $new_user_name
                RETURN u{.org_id, .user_id, .user_name, .created_at} as user
                """,
                org_id=org_id,
                user_id=user_id,
                new_user_name=new_user_name,
            )
            record = await result.single()
            return record["user"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            user_data = await session.execute_write(update_user_tx)
            if user_data is None:
                raise neo4j.exceptions.Neo4jError("User not found")
            return models.User(
                org_id=user_data["org_id"],
                user_id=user_data["user_id"],
                user_name=user_data["user_name"],
                created_at=(user_data["created_at"]).to_native(),
            )

    @override
    async def delete_user(self, org_id: str, user_id: str) -> None:
        async def delete_user_tx(tx):
            await tx.run(
                """
                MATCH (u:User {org_id: $org_id, user_id: $user_id})
                DETACH DELETE u
                """,
                org_id=org_id,
                user_id=user_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(delete_user_tx)

    @override
    async def get_user(self, org_id: str, user_id: str) -> models.User:
        async def get_user_tx(tx):
            result = await tx.run(
                """
                MATCH (u:User {org_id: $org_id, user_id: $user_id})
                RETURN u{.org_id, .user_id, .user_name, .created_at} as user
                """,
                org_id=org_id,
                user_id=user_id,
            )
            record = await result.single()
            return record["user"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            user_data = await session.execute_read(get_user_tx)
            if user_data is None:
                raise neo4j.exceptions.Neo4jError("User not found")
            return models.User(
                org_id=user_data["org_id"],
                user_id=user_data["user_id"],
                user_name=user_data["user_name"],
                created_at=(user_data["created_at"]).to_native(),
            )

    @override
    async def get_all_org_users(self, org_id: str) -> List[models.User]:
        async def get_org_users_tx(tx):
            result = await tx.run(
                """
                MATCH (o:Org {org_id: $org_id})<-[:BELONGS_TO]-(u:User)
                RETURN u{.org_id, .user_id, .user_name, .created_at} as user
                """,
                org_id=org_id,
            )
            records = await result.value("user", [])
            return records

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            users_data = await session.execute_read(get_org_users_tx)
            return [
                models.User(
                    org_id=user_data["org_id"],
                    user_id=user_data["user_id"],
                    user_name=user_data["user_name"],
                    created_at=(user_data["created_at"]).to_native(),
                )
                for user_data in users_data
            ]
