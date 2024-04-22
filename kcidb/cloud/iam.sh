# KCIDB cloud deployment - Identity and Access Management
#
if [ -z "${_IAM_SH+set}" ]; then
declare _IAM_SH=

. misc.sh

# Check if a service account exists
# Args: project name
function iam_service_account_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud iam service-accounts describe \
                --quiet --project="$project" \
                "$name@$project.iam.gserviceaccount.com" 2>&1
       ); then
        echo "true"
    elif [[ $output == *@(NOT_FOUND|PERMISSION_DENIED)* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Create a service account, if it doesn't exist
# Args: project name
function iam_service_account_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    exists=$(iam_service_account_exists "$project" "$name")
    if ! "$exists"; then
        mute gcloud iam service-accounts create \
            --quiet --project="$project" "$name"
    fi
}

# Delete a service account, if it exists
# Args: project name
function iam_service_account_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    exists=$(iam_service_account_exists "$project" "$name")
    if "$exists"; then
        mute gcloud iam service-accounts delete \
            --quiet --project="$project" \
            "$name@$project.iam.gserviceaccount.com"
    fi
}

# Deploy IAM
# Args: project grafana_service
function iam_deploy() {
    declare -r project="$1"; shift
    declare -r grafana_service="$1"; shift
    iam_service_account_deploy "$project" "$grafana_service"
    # Give Grafana access to Cloud SQL
    mute gcloud projects add-iam-policy-binding \
        --quiet --project="$project" "$project" \
        --role "roles/cloudsql.client" \
        --member \
        "serviceAccount:$grafana_service@$project.iam.gserviceaccount.com"
}

# Withdraw IAM
# Args: project grafana_service
function iam_withdraw() {
    declare -r project="$1"; shift
    declare -r grafana_service="$1"; shift
    iam_service_account_withdraw "$project" "$grafana_service"
}

fi # _IAM_SH
