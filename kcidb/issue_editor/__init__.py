#!/usr/bin/env python3
"""Kernel CI reporting - issue editor"""

import os
import json
import re
import hashlib
from datetime import datetime
from flask import Flask, request, jsonify, render_template
import kcidb

app = Flask(__name__)

project_id = os.environ.get("KCIDB_PROJECT")
topic_name = os.environ.get("KCIDB_NEW_TOPIC")
ORIGIN = "maestro"

client = kcidb.mq.IOPublisher(project_id, topic_name)


@app.route('/')
def index():
    """Root endpoint"""
    return render_template('index.html.j2')


@app.route('/submit_issue', methods=['POST'])
def submit_issue():  # pylint: disable=too-many-locals
    """Endpoint for submitting new issues"""
    def build_pattern_object(categories, fields, values):
        if not categories and not fields and not values:
            return {}
        # Initialize the automatching structure
        pattern_object = {}

        # Organize the fields based on category
        for category, field, value in zip(categories, fields, values):
            category = category + 's'
            if category not in pattern_object:
                pattern_object[category] = [{}]
            pattern_object[category][0][field] = value

        return pattern_object

    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()

    user_name = data.get('name')
    user_email = data.get('email')
    report_subject = data.get('report_subject')
    culprit_type = data.get('culprit_type')
    report_url = data.get('report_url')
    comment = data.get('comment')
    misc = data.get('misc')
    categories = request.form.getlist('category[]')
    fields = request.form.getlist('field[]')
    values = request.form.getlist('value[]')
    object_pattern = build_pattern_object(categories, fields, values)
    dry_run = data.get('dry_run', 'false') == 'true' if isinstance(
        data.get('dry_run'), str) else bool(data.get('dry_run'))

    if (not user_name or not user_email or not report_subject or
            not culprit_type):
        return jsonify({"error": "Missing required fields"}), 400

    unique_string = f"{report_subject}_" + \
        datetime.now().strftime('%Y%m%d%H%M%S')
    issue_id = hashlib.sha1(unique_string.encode()).hexdigest()

    # Additional misc information
    misc_json = json.loads(misc) if misc else {}
    misc_json.update({"author": {"name": user_name, "email": user_email}})
    if object_pattern:
        misc_json.update({"object_pattern": object_pattern})

    # Structure the report data
    issue = {
        "id": ORIGIN + ":" + issue_id,
        "version": 0,
        "origin": ORIGIN,
        "report_subject": report_subject,
        "culprit": {
            "code": culprit_type == "code",
            "tool": culprit_type == "tool",
            "harness": culprit_type == "harness"
        },
        "comment": comment,
        "misc": misc_json,
    }
    if report_url:
        issue["report_url"] = report_url

    report = {
        "version": {
            "major": 4,
            "minor": 3
        },
        "issues": [issue]
    }

    try:
        kcidb.io.SCHEMA.validate(report)
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 400

    if dry_run:
        return jsonify(report), 200

    try:
        submission_id = client.publish(report)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print("Error:", str(e))
    return jsonify({
        "submission_id": submission_id,
        "issue_id": issue["id"],
        "issue_version": issue["version"]}), 200


@app.route('/submit_incidents', methods=['POST'])
def submit_incidents():  # pylint: disable=too-many-locals
    """Endpoint for submitting new incidents"""
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()

    user_name = data.get('name')
    user_email = data.get('email')
    issue_id = data.get('issue_id')
    issue_version = int(data.get('issue_version'))
    incident_type = data.get('incident_type')
    ids_list = data.get('ids_list')
    comment = data.get('comment')
    misc = data.get('misc')
    dry_run = data.get('dry_run', 'false') == 'true' if isinstance(
        data.get('dry_run'), str) else bool(data.get('dry_run'))

    if (not user_name or  # pylint: disable=too-many-boolean-expressions
            not user_email or not issue_id or not incident_type or
            not ids_list or issue_version is None):
        return jsonify({"error": "Missing required fields"}), 400

    # Extract and clean IDs from the provided list
    ids = [id.strip() for id in re.findall(
        r'^[a-z0-9_]+:.*$', ids_list, re.MULTILINE)]
    incidents = []
    for item_id in ids:
        unique_string = f"{item_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        incident_id = hashlib.sha1(unique_string.encode()).hexdigest()

        # Additional misc information
        misc_json = json.loads(misc) if misc else {}
        misc_json.update({"author": {"name": user_name, "email": user_email}})

        # Structure the incident data
        incident = {
            "id": ORIGIN + ":" + incident_id,
            "origin": ORIGIN,
            "issue_id": issue_id,
            "issue_version": issue_version,
            "present": True,
            "comment": comment,
            "misc": misc_json
        }
        if incident_type == "build":
            incident["build_id"] = item_id
        elif incident_type == "test":
            incident["test_id"] = item_id

        incidents.append(incident)

    report = {
        "version": {
            "major": 4,
            "minor": 3
        },
        "incidents": incidents
    }

    try:
        kcidb.io.SCHEMA.validate(report)
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 400

    if dry_run:
        return jsonify(report), 200

    try:
        submission_id = client.publish(report)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print("Error:", str(e))
    return jsonify({"submission_id": submission_id}), 200
