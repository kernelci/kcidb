import os
from kcidb.tools import kcidb_match
import kcidb

CLIENT = None


def get_client():
    global CLIENT
    if not CLIENT:
        project_id = os.environ.get('GCP_PROJECT')
        topic_name = os.environ.get('KCIDB_LOAD_QUEUE_TOPIC')
        if project_id and topic_name:
            CLIENT = kcidb.Client(project_id=project_id, topic_name=topic_name)
    return CLIENT


def match_test(test):
    client = get_client()
    if client:
        incident_generator = kcidb_match.IncidentGenerator()
        incidents = incident_generator.generate_incidents_from_test(test)
        client.submit(incidents)


def match_build(build):
    client = get_client()
    if client:
        incident_generator = kcidb_match.IncidentGenerator()
        incidents = incident_generator.generate_incidents_from_build(build)
        client.submit(incidents)


def match_issue(issue):
    incident_generator = kcidb_match.IncidentGenerator()
    incident_generator.db.update_patterns(issue)
