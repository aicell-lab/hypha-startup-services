import asyncio
import os
from hypha_rpc import connect_to_server
from hypha_startup_services.register_weaviate_service import register_weaviate

SERVER_URL = "https://hypha.aicell.io"


async def register_service():
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": SERVER_URL,
            "token": token,
        }
    )
    await register_weaviate(server, "weaviate-test")
    await server.serve()


if __name__ == "__main__":
    asyncio.run(register_service())
