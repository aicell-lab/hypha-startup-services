
import argparse
import asyncio
import os
import sys
from hypha_rpc import connect_to_server, login

# We verify the service exists and (optionally) basic functionality

async def main(server_url, app_id, token):
    try:
        if token:
            client = await connect_to_server({"server_url": server_url, "token": token})
        else:
            client = await connect_to_server({"server_url": server_url})
    except Exception as e:
        print(f"Failed to connect to server: {e}")
        sys.exit(1)

    print(f"Checking service: {app_id}")
    try:
        # The app should start with the ID = app_id.
        # So we look for service `workspace/app_id`.
        # However, if `app_id` is just "weaviate", it might be `hypha-agents/weaviate`.
        # We assume the user calling this script knows the full ID or alias.
        
        # If app_id contains '/', use it as is.
        # If not, it might be in the current user's workspace? No, weaviate-app is public?
        
        svc = await client.get_service(app_id)
        print(f"Service {app_id} found.")
        
        # Check basic method
        if hasattr(svc.collections, 'list_all'):
            colls = await svc.collections.list_all()
            print(f"Collections list: {colls}")
        else:
            print("Warning: collections.list_all method not found.")
            
        print("Health check passed.")
        
    except Exception as e:
        print(f"Health check failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--token", help="Hypha token")
    args = parser.parse_args()
    
    asyncio.run(main(args.server_url, args.app_id, args.token))
