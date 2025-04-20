from typing import List, Optional, Dict, Any
import neo4j
from typing_extensions import override
from memora.schema import models
from ..base import BaseGraphDB

class Neo4jInteraction(BaseGraphDB):
    @override
    async def create_interaction(
        self,
        org_id: str,
        user_id: str,
        agent_id: str,
        interaction_id: str,
    ) -> models.Interaction:
        async def create_interaction_tx(tx):
            result = await tx.run(
                """
                MATCH (ic:InteractionCollection {org_id: $org_id, user_id: $user_id})
                CREATE (i:Interaction {
                    org_id: $org_id,
                    user_id: $user_id,
                    agent_id: $agent_id,
                    interaction_id: $interaction_id,
                    created_at: datetime(),
                    updated_at: datetime()
                })
                CREATE (ic)-[:HAD_INTERACTION]->(i)
                RETURN i{
                    .org_id,
                    .user_id,
                    .agent_id,
                    .interaction_id,
                    .created_at,
                    .updated_at
                } as interaction
                """,
                org_id=org_id,
                user_id=user_id,
                agent_id=agent_id,
                interaction_id=interaction_id,
            )
            record = await result.single()
            return record["interaction"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            interaction_data = await session.execute_write(create_interaction_tx)

            if interaction_data is None:
                raise neo4j.exceptions.Neo4jError("Failed to create interaction.")

            return models.Interaction(
                org_id=interaction_data["org_id"],
                user_id=interaction_data["user_id"],
                agent_id=interaction_data["agent_id"],
                interaction_id=interaction_data["interaction_id"],
                created_at=(interaction_data["created_at"]).to_native(),
                updated_at=(interaction_data["updated_at"]).to_native(),
                messages=[],
                memories=[],
            )

    @override
    async def get_interaction(
        self, org_id: str, user_id: str, interaction_id: str
    ) -> models.Interaction:
        async def get_interaction_tx(tx):
            result = await tx.run(
                """
                MATCH (i:Interaction {
                    org_id: $org_id,
                    user_id: $user_id,
                    interaction_id: $interaction_id
                })
                OPTIONAL MATCH (i)-[:FIRST_MESSAGE]->(first_msg:Message)
                WITH i, first_msg
                OPTIONAL MATCH path = (first_msg)-[:IS_NEXT*]->(next_msg:Message)
                WITH i, first_msg,
                     CASE WHEN first_msg IS NOT NULL
                          THEN collect(next_msg) + [first_msg]
                          ELSE []
                     END as messages
                OPTIONAL MATCH (i)-[:HAS_MEMORY]->(m:Memory)
                WITH i, messages, collect(m) as memories
                RETURN {
                    org_id: i.org_id,
                    user_id: i.user_id,
                    agent_id: i.agent_id,
                    interaction_id: i.interaction_id,
                    created_at: i.created_at,
                    updated_at: i.updated_at,
                    messages: [msg in messages | {
                        role: msg.role,
                        content: msg.content,
                        msg_position: msg.msg_position
                    }],
                    memories: [mem in memories | {
                        org_id: mem.org_id,
                        user_id: mem.user_id,
                        agent_id: mem.agent_id,
                        interaction_id: mem.interaction_id,
                        memory_id: mem.memory_id,
                        memory: mem.memory,
                        obtained_at: mem.obtained_at,
                        message_sources: mem.message_sources
                    }]
                } as interaction
                """,
                org_id=org_id,
                user_id=user_id,
                interaction_id=interaction_id,
            )
            record = await result.single()
            return record["interaction"] if record else None

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            interaction_data = await session.execute_read(get_interaction_tx)

            if interaction_data is None:
                raise neo4j.exceptions.Neo4jError("Interaction not found")

            return models.Interaction(
                org_id=interaction_data["org_id"],
                user_id=interaction_data["user_id"],
                agent_id=interaction_data["agent_id"],
                interaction_id=interaction_data["interaction_id"],
                created_at=(interaction_data["created_at"]).to_native(),
                updated_at=(interaction_data["updated_at"]).to_native(),
                messages=[
                    models.MessageBlock(
                        role=msg["role"],
                        content=msg["content"],
                        msg_position=msg["msg_position"],
                    )
                    for msg in interaction_data["messages"]
                ],
                memories=[
                    models.Memory(
                        org_id=mem["org_id"],
                        user_id=mem["user_id"],
                        agent_id=mem["agent_id"],
                        interaction_id=mem["interaction_id"],
                        memory_id=mem["memory_id"],
                        memory=mem["memory"],
                        obtained_at=(mem["obtained_at"]).to_native(),
                        message_sources=[
                            models.MessageBlock(
                                role=src["role"],
                                content=src["content"],
                                msg_position=src["msg_position"],
                            )
                            for src in (mem["message_sources"] or [])
                        ],
                    )
                    for mem in interaction_data["memories"]
                ],
            )

    @override
    async def get_all_user_interactions(
        self,
        org_id: str,
        user_id: str,
        skip: int = 0,
        limit: int = 10,
    ) -> List[models.Interaction]:
        async def get_interactions_tx(tx):
            result = await tx.run(
                """
                MATCH (ic:InteractionCollection {org_id: $org_id, user_id: $user_id})
                      -[:HAD_INTERACTION]->(i:Interaction)
                WITH i
                ORDER BY i.created_at DESC
                SKIP $skip
                LIMIT $limit
                OPTIONAL MATCH (i)-[:FIRST_MESSAGE]->(first_msg:Message)
                WITH i, first_msg
                OPTIONAL MATCH path = (first_msg)-[:IS_NEXT*]->(next_msg:Message)
                WITH i, first_msg,
                     CASE WHEN first_msg IS NOT NULL
                          THEN collect(next_msg) + [first_msg]
                          ELSE []
                     END as messages
                OPTIONAL MATCH (i)-[:HAS_MEMORY]->(m:Memory)
                WITH i, messages, collect(m) as memories
                RETURN {
                    org_id: i.org_id,
                    user_id: i.user_id,
                    agent_id: i.agent_id,
                    interaction_id: i.interaction_id,
                    created_at: i.created_at,
                    updated_at: i.updated_at,
                    messages: [msg in messages | {
                        role: msg.role,
                        content: msg.content,
                        msg_position: msg.msg_position
                    }],
                    memories: [mem in memories | {
                        org_id: mem.org_id,
                        user_id: mem.user_id,
                        agent_id: mem.agent_id,
                        interaction_id: mem.interaction_id,
                        memory_id: mem.memory_id,
                        memory: mem.memory,
                        obtained_at: mem.obtained_at,
                        message_sources: mem.message_sources
                    }]
                } as interaction
                """,
                org_id=org_id,
                user_id=user_id,
                skip=skip,
                limit=limit,
            )
            records = await result.value("interaction", [])
            return records

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.READ_ACCESS
        ) as session:
            interactions_data = await session.execute_read(get_interactions_tx)

            return [
                models.Interaction(
                    org_id=interaction["org_id"],
                    user_id=interaction["user_id"],
                    agent_id=interaction["agent_id"],
                    interaction_id=interaction["interaction_id"],
                    created_at=(interaction["created_at"]).to_native(),
                    updated_at=(interaction["updated_at"]).to_native(),
                    messages=[
                        models.MessageBlock(
                            role=msg["role"],
                            content=msg["content"],
                            msg_position=msg["msg_position"],
                        )
                        for msg in interaction["messages"]
                    ],
                    memories=[
                        models.Memory(
                            org_id=mem["org_id"],
                            user_id=mem["user_id"],
                            agent_id=mem["agent_id"],
                            interaction_id=mem["interaction_id"],
                            memory_id=mem["memory_id"],
                            memory=mem["memory"],
                            obtained_at=(mem["obtained_at"]).to_native(),
                            message_sources=[
                                models.MessageBlock(
                                    role=src["role"],
                                    content=src["content"],
                                    msg_position=src["msg_position"],
                                )
                                for src in (mem["message_sources"] or [])
                            ],
                        )
                        for mem in interaction["memories"]
                    ],
                )
                for interaction in interactions_data
            ]

    @override
    async def add_message_to_interaction(
        self,
        org_id: str,
        user_id: str,
        interaction_id: str,
        message: models.MessageBlock,
    ) -> None:
        async def add_message_tx(tx):
            # First, check if there are any existing messages
            result = await tx.run(
                """
                MATCH (i:Interaction {
                    org_id: $org_id,
                    user_id: $user_id,
                    interaction_id: $interaction_id
                })
                OPTIONAL MATCH (i)-[:FIRST_MESSAGE]->(first:Message)
                OPTIONAL MATCH path = (first)-[:IS_NEXT*]->(last:Message)
                WHERE NOT (last)-[:IS_NEXT]->()
                RETURN i, first, last
                """,
                org_id=org_id,
                user_id=user_id,
                interaction_id=interaction_id,
            )
            record = await result.single()
            
            if record is None:
                raise neo4j.exceptions.Neo4jError("Interaction not found")

            interaction = record["i"]
            first_message = record["first"]
            last_message = record["last"]

            # Create the new message
            if first_message is None:
                # This is the first message
                await tx.run(
                    """
                    MATCH (i:Interaction {
                        org_id: $org_id,
                        user_id: $user_id,
                        interaction_id: $interaction_id
                    })
                    CREATE (m:Message {
                        role: $role,
                        content: $content,
                        msg_position: $msg_position
                    })
                    CREATE (i)-[:FIRST_MESSAGE]->(m)
                    """,
                    org_id=org_id,
                    user_id=user_id,
                    interaction_id=interaction_id,
                    role=message.role,
                    content=message.content,
                    msg_position=message.msg_position,
                )
            else:
                # Add to the end of the chain
                await tx.run(
                    """
                    MATCH (i:Interaction {
                        org_id: $org_id,
                        user_id: $user_id,
                        interaction_id: $interaction_id
                    })
                    MATCH path = (i)-[:FIRST_MESSAGE|IS_NEXT*]->(last:Message)
                    WHERE NOT (last)-[:IS_NEXT]->()
                    CREATE (m:Message {
                        role: $role,
                        content: $content,
                        msg_position: $msg_position
                    })
                    CREATE (last)-[:IS_NEXT]->(m)
                    """,
                    org_id=org_id,
                    user_id=user_id,
                    interaction_id=interaction_id,
                    role=message.role,
                    content=message.content,
                    msg_position=message.msg_position,
                )

            # Update the interaction's updated_at timestamp
            await tx.run(
                """
                MATCH (i:Interaction {
                    org_id: $org_id,
                    user_id: $user_id,
                    interaction_id: $interaction_id
                })
                SET i.updated_at = datetime()
                """,
                org_id=org_id,
                user_id=user_id,
                interaction_id=interaction_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(add_message_tx)

    @override
    async def delete_interaction(
        self, org_id: str, user_id: str, interaction_id: str
    ) -> None:
        async def delete_interaction_tx(tx):
            await tx.run(
                """
                MATCH (i:Interaction {
                    org_id: $org_id,
                    user_id: $user_id,
                    interaction_id: $interaction_id
                })
                OPTIONAL MATCH (i)-[:FIRST_MESSAGE]->(first:Message)
                OPTIONAL MATCH path = (first)-[:IS_NEXT*]->(msg:Message)
                WITH i, first, collect(msg) as messages
                DETACH DELETE i, first
                WITH messages
                UNWIND messages as msg
                DETACH DELETE msg
                """,
                org_id=org_id,
                user_id=user_id,
                interaction_id=interaction_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(delete_interaction_tx)

    @override
    async def delete_all_user_interactions_and_their_memories(
        self, org_id: str, user_id: str
    ) -> None:
        async def delete_interactions_tx(tx):
            await tx.run(
                """
                MATCH (ic:InteractionCollection {org_id: $org_id, user_id: $user_id})
                      -[:HAD_INTERACTION]->(i:Interaction)
                OPTIONAL MATCH (i)-[:FIRST_MESSAGE]->(first:Message)
                OPTIONAL MATCH path = (first)-[:IS_NEXT*]->(msg:Message)
                WITH i, first, collect(msg) as messages
                OPTIONAL MATCH (i)-[:HAS_MEMORY]->(m:Memory)
                DETACH DELETE i, first, m
                WITH messages
                UNWIND messages as msg
                DETACH DELETE msg
                """,
                org_id=org_id,
                user_id=user_id,
            )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(delete_interactions_tx)

    @override
    async def delete_user_interaction_and_its_memories(
        self, org_id: str, user_id: str, interaction_id: str
    ) -> None:
        await self.delete_interaction(org_id, user_id, interaction_id)

    @override
    async def save_interaction_with_memories(
        self,
        interaction: models.Interaction,
        memories: Optional[List[models.Memory]] = None,
    ) -> None:
        async def save_interaction_tx(tx):
            # Create the interaction
            await tx.run(
                """
                MATCH (ic:InteractionCollection {org_id: $org_id, user_id: $user_id})
                CREATE (i:Interaction {
                    org_id: $org_id,
                    user_id: $user_id,
                    agent_id: $agent_id,
                    interaction_id: $interaction_id,
                    created_at: datetime(),
                    updated_at: datetime()
                })
                CREATE (ic)-[:HAD_INTERACTION]->(i)
                """,
                org_id=interaction.org_id,
                user_id=interaction.user_id,
                agent_id=interaction.agent_id,
                interaction_id=interaction.interaction_id,
            )

            # Add messages
            prev_msg = None
            for msg in interaction.messages:
                result = await tx.run(
                    """
                    CREATE (m:Message {
                        role: $role,
                        content: $content,
                        msg_position: $msg_position
                    })
                    RETURN m
                    """,
                    role=msg.role,
                    content=msg.content,
                    msg_position=msg.msg_position,
                )
                current_msg = await result.single()

                if prev_msg is None:
                    # First message
                    await tx.run(
                        """
                        MATCH (i:Interaction {
                            org_id: $org_id,
                            user_id: $user_id,
                            interaction_id: $interaction_id
                        })
                        MATCH (m:Message)
                        WHERE id(m) = $msg_id
                        CREATE (i)-[:FIRST_MESSAGE]->(m)
                        """,
                        org_id=interaction.org_id,
                        user_id=interaction.user_id,
                        interaction_id=interaction.interaction_id,
                        msg_id=current_msg["m"].id,
                    )
                else:
                    # Link to previous message
                    await tx.run(
                        """
                        MATCH (prev:Message), (curr:Message)
                        WHERE id(prev) = $prev_id AND id(curr) = $curr_id
                        CREATE (prev)-[:IS_NEXT]->(curr)
                        """,
                        prev_id=prev_msg["m"].id,
                        curr_id=current_msg["m"].id,
                    )
                prev_msg = current_msg

            # Add memories if provided
            if memories:
                for memory in memories:
                    await tx.run(
                        """
                        MATCH (i:Interaction {
                            org_id: $org_id,
                            user_id: $user_id,
                            interaction_id: $interaction_id
                        })
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
                        CREATE (i)-[:HAS_MEMORY]->(m)
                        """,
                        org_id=memory.org_id,
                        user_id=memory.user_id,
                        agent_id=memory.agent_id,
                        interaction_id=memory.interaction_id,
                        memory_id=memory.memory_id,
                        memory=memory.memory,
                        message_sources=[
                            {
                                "role": msg.role,
                                "content": msg.content,
                                "msg_position": msg.msg_position,
                            }
                            for msg in (memory.message_sources or [])
                        ],
                    )

        async with self.driver.session(
            database=self.database, default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            await session.execute_write(save_interaction_tx)

    @override
    async def update_interaction_and_memories(
        self,
        interaction: models.Interaction,
        memories: Optional[List[models.Memory]] = None,
    ) -> None:
        # First delete the existing interaction
        await self.delete_interaction(
            interaction.org_id, interaction.user_id, interaction.interaction_id
        )
        # Then save it as new
        await self.save_interaction_with_memories(interaction, memories)