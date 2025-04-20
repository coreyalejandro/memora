from typing import List, Optional, Dict, Any
import neo4j
from typing_extensions import override
from memora.schema import models
from ..base import BaseGraphDB

class Neo4jMemory(BaseGraphDB):
    @override
    async def create_memory(
        self,
        org_id: str,
        user_id: str,
        agent_id: str,
        interaction_id: str,
        memory_id: str,
        memory: str,
        message_sources: Optional[List[models.MessageBlock]] = None,
    ) -> models.Memory:
        async def create_memory_tx(tx):
            result = await tx.run(
                """
                MATCH (mc:MemoryCollection {org_id: $org_id, user_id: $user_id})
                CREATE (m:Memory {
                    org_id: $org_id,
                    user_id: $user_id,
                    agent_id: $agent_id,
                    interaction_id: $interaction_id,
                    memory_id: $memory_id,
                    memory: $memory,
                    obtained_at: datetime(),
                    message_sources: $message_sources
                })
                CREATE (mc)-[:INCLUDES]->(m)
                RETURN m{
                    .org_id,
                    .user_id,
                    .agent_id,
                    .interaction_id,
                    .memory_id,
                    .memory,
                    .obtained_at,
                    .message_sources
                } as memory
                """,
                org_id=org_id,
                user_id=user_id,
                agent_id=agent_id,
                interaction_id=interaction_id,
                memory_id=memory_id,
                memory=memory,
                message_sources=[
                    {
                        "role": msg.role,
                        "content": msg.content,
                        "msg_position": msg.msg_position,
                    }
                    for msg in (message_sources or [])
                ],
            )
            record = await result.single()
            return record["memory"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            memory_data = await session.execute_write(create_memory_tx)

            if memory_data is None:
                raise neo4j.exceptions.Neo4jError("Failed to create memory.")

            return models.Memory(
                org_id=memory_data["org_id"],
                user_id=memory_data["user_id"],
                agent_id=memory_data["agent_id"],
                interaction_id=memory_data["interaction_id"],
                memory_id=memory_data["memory_id"],
                memory=memory_data["memory"],
                obtained_at=(memory_data["obtained_at"]).to_native(),
                message_sources=[
                    models.MessageBlock(
                        role=msg["role"],
                        content=msg["content"],
                        msg_position=msg["msg_position"],
                    )
                    for msg in (memory_data["message_sources"] or [])
                ],
            )

    @override
    async def get_memory(
        self, org_id: str, user_id: str, memory_id: str
    ) -> models.Memory:
        async def get_memory_tx(tx):
            result = await tx.run(
                """
                MATCH (m:Memory {
                    org_id: $org_id,
                    user_id: $user_id,
                    memory_id: $memory_id
                })
                RETURN m{
                    .org_id,
                    .user_id,
                    .agent_id,
                    .interaction_id,
                    .memory_id,
                    .memory,
                    .obtained_at,
                    .message_sources
                } as memory
                """,
                org_id=org_id,
                user_id=user_id,
                memory_id=memory_id,
            )
            record = await result.single()
            return record["memory"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            memory_data = await session.execute_read(get_memory_tx)

            if memory_data is None:
                raise neo4j.exceptions.Neo4jError("Memory not found")

            return models.Memory(
                org_id=memory_data["org_id"],
                user_id=memory_data["user_id"],
                agent_id=memory_data["agent_id"],
                interaction_id=memory_data["interaction_id"],
                memory_id=memory_data["memory_id"],
                memory=memory_data["memory"],
                obtained_at=(memory_data["obtained_at"]).to_native(),
                message_sources=[
                    models.MessageBlock(
                        role=msg["role"],
                        content=msg["content"],
                        msg_position=msg["msg_position"],
                    )
                    for msg in (memory_data["message_sources"] or [])
                ],
            )

    @override
    async def get_all_user_memories(
        self,
        org_id: str,
        user_id: str,
        agent_id: Optional[str] = None,
        skip: int = 0,
        limit: int = 10,
    ) -> List[models.Memory]:
        async def get_all_memories_tx(tx):
            agent_filter = ""
            if agent_id:
                agent_filter = "AND m.agent_id = $agent_id"

            query = """
                MATCH (mc:MemoryCollection {org_id: $org_id, user_id: $user_id})
                      -[:INCLUDES]->(m:Memory)
                WHERE true {}
                WITH m
                ORDER BY m.obtained_at DESC
                SKIP $skip
                LIMIT $limit
                RETURN m{{
                    .org_id,
                    .user_id,
                    .agent_id,
                    .interaction_id,
                    .memory_id,
                    .memory,
                    .obtained_at,
                    .message_sources
                }} as memory
            """.format(
                agent_filter
            )

            result = await tx.run(
                query,
                org_id=org_id,
                user_id=user_id,
                agent_id=agent_id,
                skip=skip,
                limit=limit,
            )
            records = await result.value("memory", [])
            return records

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            memories_data = await session.execute_read(get_all_memories_tx)

            return [
                models.Memory(
                    org_id=memory["org_id"],
                    user_id=memory["user_id"],
                    agent_id=memory["agent_id"],
                    interaction_id=memory["interaction_id"],
                    memory_id=memory["memory_id"],
                    memory=memory["memory"],
                    obtained_at=(memory["obtained_at"]).to_native(),
                    message_sources=[
                        models.MessageBlock(
                            role=msg["role"],
                            content=msg["content"],
                            msg_position=msg["msg_position"],
                        )
                        for msg in (memory["message_sources"] or [])
                    ],
                )
                for memory in memories_data
            ]

    @override
    async def delete_memory(
        self, org_id: str, user_id: str, memory_id: str
    ) -> None:
        async def delete_memory_tx(tx):
            await tx.run(
                """
                MATCH (m:Memory {
                    org_id: $org_id,
                    user_id: $user_id,
                    memory_id: $memory_id
                })
                DETACH DELETE m
                """,
                org_id=org_id,
                user_id=user_id,
                memory_id=memory_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(delete_memory_tx)

    @override
    async def delete_all_user_memories(self, org_id: str, user_id: str) -> None:
        async def delete_memories_tx(tx):
            await tx.run(
                """
                MATCH (mc:MemoryCollection {org_id: $org_id, user_id: $user_id})
                      -[:INCLUDES]->(m:Memory)
                DETACH DELETE m
                """,
                org_id=org_id,
                user_id=user_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(delete_memories_tx)

    @override
    async def delete_user_memory(self, org_id: str, user_id: str, memory_id: str) -> None:
        await self.delete_memory(org_id, user_id, memory_id)

    @override
    async def get_user_memory(self, org_id: str, user_id: str, memory_id: str) -> models.Memory:
        return await self.get_memory(org_id, user_id, memory_id)

    @override
    async def get_user_memory_history(
        self, org_id: str, user_id: str, skip: int = 0, limit: int = 10
    ) -> List[models.Memory]:
        return await self.get_all_user_memories(org_id, user_id, skip=skip, limit=limit)

    @override
    async def fetch_user_memories_resolved(
        self, org_id: str, user_id: str, memory_ids: List[str]
    ) -> List[models.Memory]:
        async def fetch_memories_tx(tx):
            result = await tx.run(
                """
                MATCH (m:Memory)
                WHERE m.org_id = $org_id 
                AND m.user_id = $user_id
                AND m.memory_id IN $memory_ids
                RETURN m{
                    .org_id,
                    .user_id,
                    .agent_id,
                    .interaction_id,
                    .memory_id,
                    .memory,
                    .obtained_at,
                    .message_sources
                } as memory
                """,
                org_id=org_id,
                user_id=user_id,
                memory_ids=memory_ids,
            )
            records = await result.value("memory", [])
            return records

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            memories_data = await session.execute_read(fetch_memories_tx)
            return [
                models.Memory(
                    org_id=memory["org_id"],
                    user_id=memory["user_id"],
                    agent_id=memory["agent_id"],
                    interaction_id=memory["interaction_id"],
                    memory_id=memory["memory_id"],
                    memory=memory["memory"],
                    obtained_at=(memory["obtained_at"]).to_native(),
                    message_sources=[
                        models.MessageBlock(
                            role=msg["role"],
                            content=msg["content"],
                            msg_position=msg["msg_position"],
                        )
                        for msg in (memory["message_sources"] or [])
                    ],
                )
                for memory in memories_data
            ]

    @override
    async def fetch_user_memories_resolved_batch(
        self, memories_to_fetch: List[Dict[str, str]]
    ) -> List[models.Memory]:
        all_memories = []
        for memory_info in memories_to_fetch:
            try:
                memory = await self.get_user_memory(
                    memory_info["org_id"],
                    memory_info["user_id"],
                    memory_info["memory_id"],
                )
                all_memories.append(memory)
            except neo4j.exceptions.Neo4jError:
                continue
        return all_memories
