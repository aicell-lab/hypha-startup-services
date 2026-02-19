"""Background offloader for unused applications."""

import time

OFFLOAD_AFTER = 600
RUN_FREQUENCY = 60


# TODO: actually implement and integrate this offloader
def background_offloader() -> None:
    """Background thread that offloads unused applications."""
    while True:
        time.sleep(RUN_FREQUENCY)
        now = time.time()
        to_offload = []

        for app_id, last_time in list(last_used.items()):
            if now - last_time > OFFLOAD_AFTER:
                to_offload.append(app_id)

        # Do actual offloading outside the lock to avoid blocking main thread
        for app_id in to_offload:
            offload(app_id)
            del last_used[app_id]
