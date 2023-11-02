# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# Resource Constants
RESOURCE_BACKFILL = "Backfills"

RESOURCE_BACKFILL_MENU_CATEGORY = "Backfill"
RESOURCE_BACKFILL_MENU_LIST_NAME = "List Backfills"

ACTION_CAN_OPERATE = "can_operate"


def grant_backfill_permissions(sm, permitted_roles):
    import logging
    from airflow.security import permissions as airflow_permissions

    permissions = []
    # menu permissions
    permissions.extend([
        (airflow_permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_BACKFILL_MENU_CATEGORY),
        (airflow_permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_BACKFILL_MENU_LIST_NAME),
    ])

    # resource permissions
    permissions.extend([
        (airflow_permissions.ACTION_CAN_CREATE, RESOURCE_BACKFILL),
        (airflow_permissions.ACTION_CAN_READ, RESOURCE_BACKFILL),
        (ACTION_CAN_OPERATE, RESOURCE_BACKFILL),
        (airflow_permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_BACKFILL),
    ])

    perms = [
        sm.get_permission(action_name, resource_name) or sm.create_permission(action_name, resource_name)
        for action_name, resource_name in permissions
    ]

    for role_name in permitted_roles:
        role = sm.find_role(role_name)
        if not role:
            logging.warn(f"role {role_name} does not exist, skipped")
            continue
        for perm in perms:
            if perm not in role.permissions:
                sm.add_permission_to_role(role, perm)
