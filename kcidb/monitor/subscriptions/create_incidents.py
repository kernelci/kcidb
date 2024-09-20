from kcidb.tools import kcidb_match
import kcidb
import json


def match_test(test):
    kcidb_io_object = {"tests": [test._data],
                       "builds": [test.build._data],
                       "checkouts": [test.build.checkout._data]}
    incident_generator = kcidb_match.IncidentGenerator()
    incidents = incident_generator.generate_incidents_from_db(kcidb_io_object)
    #if (incidents["incidents"]):
    #    kcidb.Client.submit(incidents)
    # TODO: tmp solution
    json.dumps(incidents)


def match_build(build):
    kcidb_io_object = {"builds": [build._data],
                       "checkouts": [build.checkout._data]}
    incident_generator = kcidb_match.IncidentGenerator()
    incidents = incident_generator.generate_incidents_from_db(kcidb_io_object)
    #if (incidents["incidents"]):
    #    kcidb.Client.submit(incidents)
    # TODO: tmp solution
    json.dumps(incidents)


def match_issues(issues):
    issue_objects = {"issues": [issues._data]}
    incident_generator = kcidb_match.IncidentGenerator()
    incident_generator.db.update_patterns(issue_objects)
