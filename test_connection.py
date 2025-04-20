import asyncio
from dotenv import load_dotenv
import os
from pathlib import Path

from qdrant_client import AsyncQdrantClient
from memora.llm_backends import OpenAIBackendLLM
from memora.vector_db import QdrantDB

async def test_neo4j_connection():
    print("Initializing Neo4j connection...")
    
    # Print current working directory and env file status
    print(f"Current directory: {os.getcwd()}")
    print(f"Env file exists: {Path('.env').exists()}")
    
    # Load environment variables and print them
    load_dotenv(verbose=True)
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    database = os.getenv("NEO4J_DATABASE")
    
    print(f"Debug - URI: {uri!r}")
    print(f"Debug - Username: {username!r}")
    print(f"Debug - Database: {database!r}")
    
    if not uri:
        raise ValueError("NEO4J_URI environment variable is not set or is empty")

    from memora import Memora
    from memora.graph_db import Neo4jGraphInterface
    
    # Initialize Neo4j connection
    graph_db = Neo4jGraphInterface(
        uri=uri,
        username=username,
        password=os.getenv("NEO4J_PASSWORD"),
        database=database,
    )

    # Initialize vector database (using a mock URL for testing)
    vector_db = QdrantDB(
        async_client=AsyncQdrantClient(url="http://localhost:6333")
    )
    
    # Initialize Memora with all required arguments
    memora = Memora(
        vector_db=vector_db,
        graph_db=graph_db,
        # Using mock API key for testing
        memory_search_model=OpenAIBackendLLM(api_key="test", model="gpt-4"),
        extraction_model=OpenAIBackendLLM(api_key="test", model="gpt-4"),
        enable_logging=True,
    )
    
    try:
        # Test organization creation
        org_id = os.getenv("ORG_ID")
        org = await memora.graph.create_organization("Test Organization")
        print(f"✅ Successfully created organization: {org.org_id}")
        
        # Test user creation
        user_id = os.getenv("USER_ID")
        user = await memora.graph.create_user(org.org_id, "Test User")
        print(f"✅ Successfully created user: {user.user_id}")
        
        print("\nConnection test successful! Neo4j is properly configured.")
        
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
    finally:
        # Clean up
        await graph_db.close()

if __name__ == "__main__":
    asyncio.run(test_neo4j_connection())
