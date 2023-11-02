# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow.plugins_manager import AirflowPlugin
from flask import current_app


class AirflowBackfillPlugin(AirflowPlugin):
    name = "backfill_plugin"

    def on_load(*args, **kwargs):
        AirflowPlugin.on_load(args, kwargs)

        # only init backfill views in webserver
        if current_app:
            from linkedin.airflow.backfill.www.views import init_views
            init_views()

        # init for backfill
        from linkedin.airflow.backfill.models.backfill import init_models
        init_models()

    # backfill plugins are only for webserver
    if current_app:
        from flask import Blueprint
        from linkedin.airflow.backfill.www.views import BackfillView
        import linkedin.airflow.backfill.security.permissions as backfill_permissions
        from linkedin.airflow.backfill.apis.backfill_api import backfill_api_bp

        backfill_bp = Blueprint("backfill_plugin", __name__, template_folder="www/templates")
        backfill_view = BackfillView()
        backfill_package = {
            "name": backfill_permissions.RESOURCE_BACKFILL_MENU_LIST_NAME,      # Name under tab
            "category": backfill_permissions.RESOURCE_BACKFILL_MENU_CATEGORY,   # Name of menu link
            "view": backfill_view
        }

        flask_blueprints = [backfill_bp, backfill_api_bp]
        appbuilder_views = [backfill_package]
