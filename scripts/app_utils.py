
import os
import re

def get_branch_slug(ref_name):
    # Slugify the branch name
    slug = ref_name.lower()
    slug = re.sub(r'[^a-z0-9-]', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')

def get_app_id(base_id, branch_slug):
    app_id = f"{base_id}-{branch_slug}"
    # Cap length at 63 chars (Hypha/DNS limitation often) - though check specific limits
    if len(app_id) > 63:
        app_id = app_id[:63].rstrip('-')
    return app_id
