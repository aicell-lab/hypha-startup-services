import os
import sys
from typing import Any
import argparse
from argparse import Namespace
import asyncio
import subprocess
from websockets.exceptions import InvalidURI
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService, RemoteException
from hypha_startup_services.register_mem0_service import register_mem0

DEFAULT_SERVICE_ID = "mem0-test"


async def connect_to_server_and_execute(
    provided_url: str, port: int | None = None, action_func=None, **kwargs
) -> RemoteService:
    """Generic function to connect to server and execute an action.

    Args:
        provided_url (str): The URL of the server to connect to.
        port (int, optional): The port of the server. Defaults to None.
        action_func: The function to execute after connecting to the server.
        **kwargs: Additional arguments to pass to the action function.
    """
    server_url = provided_url if port is None else f"{provided_url}:{port}"
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server({"server_url": server_url, "token": token})

    if not isinstance(server, RemoteService):
        raise ValueError("Server is not a RemoteService instance.")

    if action_func:
        return await action_func(server, **kwargs)
    return server


async def register_mem0_action(server: RemoteService, service_id: str) -> None:
    """Action function to register mem0 service."""
    await register_mem0(server, service_id)


async def get_deno_service(server: RemoteService) -> RemoteService:
    """Action function to get deno-app-engine service."""
    deno_service = await server.get_service(
        "hypha-agents/deno-app-engine",
        config={"mode": "random", "timeout": 10.0},
    )
    print(f"Connected to deno-app-engine service: {deno_service.id}")
    return deno_service


def get_installation_code() -> str:
    """Return the code to install required packages."""
    return """
import subprocess
import sys

packages = [
    "mem0ai", 
    "hypha-rpc", 
    "python-dotenv",
    "git+https://github.com/aicell-lab/hypha-startup-services.git"  # Replace with actual repo
]

for package in packages:
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
"""


def get_registration_code(server: RemoteService, service_id: str) -> str:
    """Return the code to register the mem0 service."""
    print(
        f"Registering mem0 service with ID: {service_id} on server: {server.config.public_base_url}"
    )
    return f"""import os
import asyncio
from hypha_rpc import connect_to_server
from hypha_startup_services.register_mem0_service import register_mem0

token = os.environ.get("HYPHA_TOKEN")

async def register_mem0_service():
    server = await connect_to_server({{
        "server_url": "{server.config.public_base_url}",
        "workspace": "{server.config.workspace}",
        "token": token
    }})
    
    await register_mem0(server, "{service_id}")

# Run the registration
asyncio.run(register_mem0_service())
"""


async def manage_existing_kernel(
    deno_service: RemoteService, kernel_id: str, destroy: bool = False
) -> bool:
    """Manage existing kernel for the service.

    Args:
        deno_service: The deno-app-engine service
        kernel_id: The kernel ID
        destroy: If True, destroy the kernel completely. If False, restart it.

    Returns:
        True if a kernel was found and managed, False if no existing kernel
    """
    try:
        if destroy:
            await deno_service.destroyKernel({"kernelId": kernel_id})
            print(f"Destroyed kernel: {kernel_id}")
        else:
            await deno_service.interruptKernel({"kernelId": kernel_id})
            print(f"Stopped kernel: {kernel_id}")

        return True

    except RemoteException as e:
        print(f"No existing kernel found or error managing kernel: {e}")
        return False


async def create_deno_kernel(
    deno_service: RemoteService, service_id: str
) -> dict[str, Any]:
    """Create a Deno kernel for the specified service."""
    kernel_info = await deno_service.createKernel(
        {
            "id": f"mem0-kernel-{service_id}",
            "mode": "worker",  # Use worker mode for isolation
            "inactivity_timeout": 3600000,  # 1 hour timeout
            "max_execution_time": 300000,  # 5 minutes max execution time
        }
    )
    print(f"Created kernel: {kernel_info}")
    return kernel_info


async def install_deno_dependencies(
    deno_service: RemoteService, kernel_id: str
) -> dict[str, Any]:
    """Install necessary dependencies for the deno service."""
    # Step 1: Install dependencies
    install_code = get_installation_code()

    install_result = await deno_service.executeCode(
        {"kernelId": kernel_id, "code": install_code}
    )
    print(f"Installation execution: {install_result}")

    return install_result


async def run_mem0_on_deno_action(
    server: RemoteService, service_id: str, destroy: bool = False
) -> dict[str, Any]:
    """Action function to run mem0 service on deno-app-engine."""
    # Get the deno-app-engine service
    deno_service = await get_deno_service(server)

    kernel_id = f"mem0-kernel-{service_id}"

    kernel_existed = await manage_existing_kernel(deno_service, kernel_id, destroy)

    if not kernel_existed:
        await create_deno_kernel(deno_service, service_id)

    install_result = await install_deno_dependencies(deno_service, kernel_id)

    register_code = get_registration_code(server, service_id)

    register_result = await deno_service.executeCode(
        {"kernelId": kernel_id, "code": register_code}
    )

    action_msg = "restarted and set up" if kernel_existed else "created and set up"
    print(f"Mem0 service has been {action_msg} on kernel {kernel_id}")

    return {
        "deno_service": deno_service,
        "kernel_id": kernel_id,
        "install_result": install_result,
        "register_result": register_result,
        "service_url": f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
        "kernel_existed": kernel_existed,
    }


async def register_to_existing_server(
    provided_url: str, service_id: str, port: int | None = None
) -> RemoteService:
    """Register to an existing server with mem0 service."""
    return await connect_to_server_and_execute(
        provided_url, port, register_mem0_action, service_id=service_id
    )


async def run_local_server(
    server_url: str, port: int, service_id_existing_server: str
) -> None:
    """Run the local server with the provided arguments.

    Args:
        server_url (str): The URL of the server to connect to.
        port (int): The port of the server.
        service_id_existing_server (str): The ID of the service.
    """
    try:
        await register_to_existing_server(
            server_url, service_id_existing_server, port=port
        )
    except (ConnectionRefusedError, InvalidURI, RemoteException):
        command = [
            sys.executable,
            "-m",
            "hypha.server",
            f"--host={server_url}",
            f"--port={port}",
            "--startup-functions=hypha_startup_services.register_mem0_service:register_mem0",
        ]
        subprocess.run(command, check=True)


def run_remote_service(args: Namespace, action_func, **kwargs) -> None:
    """Generic function to run a remote service action.

    Args:
        args (Namespace): The arguments parsed from the command line.
        action_func: The async action function to execute.
        **kwargs: Additional arguments to pass to the action function.
    """
    server_url = args.server_url
    port = getattr(args, "port", None)
    loop = asyncio.get_event_loop()
    loop.create_task(
        connect_to_server_and_execute(server_url, port, action_func, **kwargs)
    )
    loop.run_forever()


def connect_to_remote(args: Namespace) -> None:
    """Connect to remote server and register mem0 service."""
    service_id = args.service_id
    run_remote_service(args, register_mem0_action, service_id=service_id)


def connect_to_deno(args: Namespace) -> None:
    """Connect to the deno-app-engine service and run mem0 service on it."""
    service_id = getattr(args, "service_id", DEFAULT_SERVICE_ID)
    destroy = getattr(args, "destroy", False)
    run_remote_service(
        args, run_mem0_on_deno_action, service_id=service_id, destroy=destroy
    )


def run_locally(args: Namespace) -> None:
    """Run the local server with the provided arguments.

    Args:
        args (Namespace): The arguments parsed from the command line.
    """
    asyncio.run(
        run_local_server(args.server_url, args.port, args.service_id_existing_server)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hypha startup services launch commands."
    )

    subparsers = parser.add_subparsers()

    parser_local = subparsers.add_parser("local")
    parser_local.add_argument("--server-url", type=str, default="127.0.0.1")
    parser_local.add_argument("--port", type=int, default=9527)
    parser_local.add_argument(
        "--service-id-existing-server",
        type=str,
        default=DEFAULT_SERVICE_ID,
    )
    parser_local.set_defaults(func=run_locally)

    parser_remote = subparsers.add_parser("remote")
    parser_remote.add_argument(
        "--server-url", type=str, default="https://hypha.aicell.io"
    )
    parser_remote.add_argument(
        "--port", type=int, help="Port number for the server connection"
    )
    parser_remote.add_argument(
        "--service-id",
        type=str,
        default=DEFAULT_SERVICE_ID,
    )
    parser_remote.set_defaults(func=connect_to_remote)

    parser_deno = subparsers.add_parser("deno")
    parser_deno.add_argument(
        "--server-url", type=str, default="https://hypha.aicell.io"
    )
    parser_deno.add_argument(
        "--port", type=int, help="Port number for the server connection"
    )
    parser_deno.add_argument(
        "--service-id",
        type=str,
        default=DEFAULT_SERVICE_ID,
        help="Service ID for the mem0 service",
    )
    parser_deno.add_argument(
        "--destroy",
        action="store_true",
        help="Destroy existing kernel completely instead of restarting it",
    )
    parser_deno.set_defaults(func=connect_to_deno)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
