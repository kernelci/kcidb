"""Automate incident creation"""
import os
import kcidb
from kcidb.tools import kcidb_match

CLIENT = None


# pylint: disable=global-statement
def get_client():
    """Get KCIDB client instance and set it as a global variable"""
    global CLIENT
    if not CLIENT:
        project_id = os.environ.get('GCP_PROJECT')
        topic_name = os.environ.get('KCIDB_LOAD_QUEUE_TOPIC')
        if project_id and topic_name:
            CLIENT = kcidb.Client(project_id=project_id, topic_name=topic_name)
    return CLIENT


def match_test(test):
    """Generate incident for matching test"""
    client = get_client()
    if client:
        incident_generator = kcidb_match.IncidentGenerator()
        incidents = incident_generator.generate_incidents_from_test(test)
        client.submit(incidents)


def match_build(build):
    """Generate incident for matching build"""
    client = get_client()
    if client:
        incident_generator = kcidb_match.IncidentGenerator()
        incidents = incident_generator.generate_incidents_from_build(build)
        client.submit(incidents)


def match_issue(issue):
    """Match issue and add its pattern to DB"""
    incident_generator = kcidb_match.IncidentGenerator()
    incident_generator.db.update_patterns(issue)
