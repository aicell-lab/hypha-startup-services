import argparse
import asyncio
import sys
from collections.abc import Sequence

try:
    from scripts.hypha_connection import connect_with_fallback
except ModuleNotFoundError:
    from hypha_connection import connect_with_fallback


def _build_candidate_service_ids(
    app_id: str,
    service_ids: Sequence[str],
) -> list[str]:
    """Build prioritized list of service ids to probe."""
    candidates = [app_id, *service_ids]
    unique_candidates = list(dict.fromkeys(candidates))
    return unique_candidates


async def _get_first_resolvable_service(
    client,
    candidate_service_ids: Sequence[str],
):
    """Resolve and return the first service id that exists."""
    last_error: Exception | None = None
    for service_id in candidate_service_ids:
        try:
            service = await client.get_service(service_id)
        except Exception as error:  # noqa: BLE001
            last_error = error
            continue
        print(f"Service {service_id} found.")
        return service

    if last_error is not None:
        raise last_error
    raise RuntimeError("No candidate service ids provided.")


async def main(
    server_url: str,
    app_id: str,
    token: str | None,
    service_ids: Sequence[str],
) -> None:
    try:
        client = await connect_with_fallback(server_url=server_url, token=token)
    except Exception as e:
        print(f"Failed to connect to server: {e}")
        sys.exit(1)

    candidate_service_ids = _build_candidate_service_ids(app_id, service_ids)
    print(f"Checking candidate services: {candidate_service_ids}")
    try:
        svc = await _get_first_resolvable_service(client, candidate_service_ids)

        # Check basic method
        if hasattr(svc.collections, "list_all"):
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
    parser.add_argument(
        "--service-id",
        action="append",
        default=[],
        help="Additional service id candidates to check.",
    )
    args = parser.parse_args()

    asyncio.run(
        main(
            args.server_url,
            args.app_id,
            args.token,
            args.service_id,
        ),
    )
