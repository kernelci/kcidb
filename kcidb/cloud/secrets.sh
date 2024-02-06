# KCIDB cloud deployment - secrets
#
if [ -z "${_SECRETS_SH+set}" ]; then
declare _SECRETS_SH=

. misc.sh

# The name of the shared SMTP password secret
declare -r SECRETS_SMTP_PASSWORD="kcidb_smtp_password"

# Deploy secrets
# Args: project
#       psql_pgpass_secret
#       psql_editor_username
#       grafana_service
function secrets_deploy() {
    declare -r project="$1"; shift
    declare -r psql_pgpass_secret="$1"; shift
    declare -r psql_editor_username="$1"; shift
    declare -r grafana_service="$1"; shift
    declare exists

    # Make sure the shared SMTP password secret is deployed
    password_secret_deploy smtp
    # Give Cloud Functions access to the shared SMTP password secret
    mute gcloud secrets add-iam-policy-binding \
        --quiet --project="$project" "$SECRETS_SMTP_PASSWORD" \
        --role roles/secretmanager.secretAccessor \
        --member "serviceAccount:$project@appspot.gserviceaccount.com"

    # Make sure all PostgreSQL's password secrets are deployed
    password_secret_deploy psql_superuser psql_editor psql_viewer psql_grafana
    # DO NOT give Cloud Functions access to *any* PostgreSQL password secrets

    # Make sure PostgreSQL's .pgpass secret is deployed
    password_secret_deploy_pgpass "$project" "$psql_pgpass_secret" \
        psql_editor "$psql_editor_username"

    # Give Cloud Functions access to the .pgpass secret
    mute gcloud secrets add-iam-policy-binding \
        --quiet --project="$project" "$psql_pgpass_secret" \
        --role roles/secretmanager.secretAccessor \
        --member "serviceAccount:$project@appspot.gserviceaccount.com"

    # Give Grafana access to its PostgreSQL password
    mute gcloud secrets add-iam-policy-binding \
        --quiet --project="$project" \
        "$(password_secret_get_name psql_grafana)" \
        --role roles/secretmanager.secretAccessor \
        --member \
        "serviceAccount:$grafana_service@$project.iam.gserviceaccount.com"
}

# Withdraw secrets
# Args: project
#       psql_pgpass_secret
function secrets_withdraw() {
    declare -r project="$1"; shift
    declare -r psql_pgpass_secret="$1"; shift
    password_secret_withdraw psql_editor psql_grafana
    secret_withdraw "$project" "$psql_pgpass_secret"
    # NOTE: Not withdrawing the shared secrets
}

fi # _SECRETS_SH
