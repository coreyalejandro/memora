from typing import List
import neo4j
import neo4j.exceptions
import shortuuid
from typing_extensions import override
from memora.schema import models
from ..base import BaseGraphDB

class Neo4jOrganization(BaseGraphDB):
    @override
    async def create_organization(self, org_name: str) -> models.Organization:
        if not isinstance(org_name, str) or not org_name:
            raise TypeError("`org_name` must be a string and have a value.")
        
        org_id = shortuuid.uuid()
        self.logger.info(f"Creating organization with ID {org_id}")
        
        async def create_org_tx(tx):
            result = await tx.run(
                """
                CREATE (o:Org {
                    org_id: $org_id,
                    org_name: $org_name,
                    created_at: datetime()
                })
                RETURN o{.org_id, .org_name, .created_at} as org
                """,
                org_id=org_id,
                org_name=org_name,
            )
            
            record = await result.single()
            return record["org"] if record else None
            
        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            org_data = await session.execute_write(create_org_tx)
            
            if org_data is None:
                raise neo4j.exceptions.Neo4jError("Failed to create organization.")
                
            return models.Organization(
                org_id=org_data["org_id"],
                org_name=org_data["org_name"],
                created_at=(org_data["created_at"]).to_native(),
            )

    @override
    async def update_organization(self, org_id: str, new_org_name: str) -> models.Organization:
        async def update_org_tx(tx):
            result = await tx.run(
                """
                MATCH (o:Org {org_id: $org_id})
                SET o.org_name = $new_org_name
                RETURN o{.org_id, .org_name, .created_at} as org
                """,
                org_id=org_id,
                new_org_name=new_org_name,
            )
            record = await result.single()
            return record["org"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            org_data = await session.execute_write(update_org_tx)
            if org_data is None:
                raise neo4j.exceptions.Neo4jError("Organization not found")
            return models.Organization(
                org_id=org_data["org_id"],
                org_name=org_data["org_name"],
                created_at=(org_data["created_at"]).to_native(),
            )

    @override
    async def delete_organization(self, org_id: str) -> None:
        async def delete_org_tx(tx):
            await tx.run(
                """
                MATCH (o:Org {org_id: $org_id})
                DETACH DELETE o
                """,
                org_id=org_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(delete_org_tx)

    @override
    async def get_organization(self, org_id: str) -> models.Organization:
        async def get_org_tx(tx):
            result = await tx.run(
                """
                MATCH (o:Org {org_id: $org_id})
                RETURN o{.org_id, .org_name, .created_at} as org
                """,
                org_id=org_id,
            )
            record = await result.single()
            return record["org"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            org_data = await session.execute_read(get_org_tx)
            if org_data is None:
                raise neo4j.exceptions.Neo4jError("Organization not found")
            return models.Organization(
                org_id=org_data["org_id"],
                org_name=org_data["org_name"],
                created_at=(org_data["created_at"]).to_native(),
            )

    @override
    async def get_all_organizations(self) -> List[models.Organization]:
        async def get_all_orgs_tx(tx):
            result = await tx.run(
                """
                MATCH (o:Org)
                RETURN o{.org_id, .org_name, .created_at} as org
                """
            )
            records = await result.value("org", [])
            return records

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            orgs_data = await session.execute_read(get_all_orgs_tx)
            return [
                models.Organization(
                    org_id=org_data["org_id"],
                    org_name=org_data["org_name"],
                    created_at=(org_data["created_at"]).to_native(),
                )
                for org_data in orgs_data
            ]
