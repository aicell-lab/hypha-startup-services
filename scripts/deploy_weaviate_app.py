
import argparse
import asyncio
import logging
import os
from pathlib import Path

from hypha_rpc import connect_to_server

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def deploy_app(server_url, token, app_id, source_path, manifest_path):
    client = await connect_to_server({"server_url": server_url, "token": token})
    
    # Read source
    with open(source_path, "r") as f:
        source_code = f.read()
            
    # Prepare manifest
    import yaml
    with open(manifest_path, "r") as f:
        manifest_data = yaml.safe_load(f)

    if not manifest_data.get("type"):
        manifest_data["type"] = "hypha"

    source_entry_point = Path(source_path).name
    manifest_data["entry_point"] = source_entry_point

    # Inject current git branch if in CI
    # This ensures the app uses the code from the PR/branch being tested
    head_ref = os.environ.get("GITHUB_HEAD_REF") or os.environ.get("GITHUB_REF_NAME")
    if head_ref and "hypha-startup-services" in str(manifest_data):
        logger.info(f"Injecting git branch '{head_ref}' into manifest dependencies...")
        # Recursively find and replace in dependencies
        def inject_branch(obj):
            if isinstance(obj, list):
                for i, item in enumerate(obj):
                    if isinstance(item, str) and "hypha-startup-services.git" in item:
                        # Replace @main or similar with @branch
                        if "@" in item:
                            base = item.split("@")[0]
                            obj[i] = f"{base}@{head_ref}"
                            logger.info(f"Replaced dependency: {item} -> {obj[i]}")
                    else:
                        inject_branch(item)
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    inject_branch(v)

        inject_branch(manifest_data)
        
    server_apps = await client.get_service("public/server-apps")
        
    await server_apps.install(
        app_id=app_id,
        source=source_code,
        manifest=manifest_data,
        overwrite=True
    )
    
    logger.info(f"App {app_id} installed.")
    
    # Start the app
    logger.info(f"Starting app {app_id}...")
    await server_apps.start(app_id)
    logger.info(f"App {app_id} started.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--non-fatal-start", action="store_true", help="Don't fail if start times out (start async)")

    args = parser.parse_args()
    
    # We ignore non-fatal-start for now in implementation logic (handled by caller or simple run)
    asyncio.run(deploy_app(args.server_url, args.token, args.app_id, args.source, args.manifest))
