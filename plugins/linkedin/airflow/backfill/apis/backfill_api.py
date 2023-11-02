# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from flask import Blueprint, request, g, current_app, jsonify
# from airflow.www.app import csrf
from airflow.api_connexion import security
from airflow.api_connexion.exceptions import common_error_handler
from airflow.security import permissions
import linkedin.airflow.backfill.security.permissions as backfill_permissions
from linkedin.airflow.backfill.apis import api_impl as impl


# Create a flask blueprint for the upload API
backfill_api_bp = Blueprint(
    "backfill_plugin_api",
    __name__,
    url_prefix="/api/v1/plugins/backfills"
)


@backfill_api_bp.app_errorhandler(400)
@backfill_api_bp.app_errorhandler(401)
@backfill_api_bp.app_errorhandler(403)
@backfill_api_bp.app_errorhandler(404)
@backfill_api_bp.app_errorhandler(409)
@backfill_api_bp.app_errorhandler(500)
def handle_errors(ex):
    return common_error_handler(ex)


@backfill_api_bp.route("", methods=['GET'])
@security.requires_access([(permissions.ACTION_CAN_READ, backfill_permissions.RESOURCE_BACKFILL)])
def list_backfills():
    """
    list backfills, filters: dag_id, start_date, end_date, include_deleted
    """
    permitted_dag_ids_set = set(current_app.appbuilder.sm.get_readable_dag_ids(g.user))
    return jsonify(impl.list_backfills(permitted_dag_ids_set, request.args)), 200


@backfill_api_bp.route("/<string:backfill_dag_id>", methods=['GET'])
@security.requires_access([(permissions.ACTION_CAN_READ, backfill_permissions.RESOURCE_BACKFILL)])
def get_backfill(backfill_dag_id):
    """
    get backfill info
    """
    backfill = impl.get_backfill(backfill_dag_id)
    return jsonify(backfill), 200


@backfill_api_bp.route("", methods=['POST'])
# @csrf.exempt
@security.requires_access([(permissions.ACTION_CAN_CREATE, backfill_permissions.RESOURCE_BACKFILL)])
def create_backfill():
    """
    create backfill, return backfill object
    """
    data = request.json
    permitted_dag_ids_set = set(current_app.appbuilder.sm.get_editable_dag_ids(g.user))
    backfill = impl.create_backfill(permitted_dag_ids_set, data, current_app.dag_bag, g.user.username)
    return jsonify(backfill), 200


@backfill_api_bp.route("/<string:backfill_dag_id>/cancel", methods=['POST'])
# @csrf.exempt
@security.requires_access([(backfill_permissions.ACTION_CAN_OPERATE, backfill_permissions.RESOURCE_BACKFILL)])
def cancel_backfill(backfill_dag_id):
    """
    cancel submitted backfill or cancel backfill dag runs, return backfill object
    """
    backfill = impl.cancel_backfill(backfill_dag_id, current_app.dag_bag)
    return jsonify(backfill), 200


@backfill_api_bp.route("/<string:backfill_dag_id>", methods=['DELETE'])
# @csrf.exempt
@security.requires_access([(backfill_permissions.ACTION_CAN_OPERATE, backfill_permissions.RESOURCE_BACKFILL)])
def delete_backfill(backfill_dag_id):
    """
    mark delete backfill, return backfill object
    """
    backfill = impl.delete_backfill(backfill_dag_id)
    return jsonify(backfill), 200
