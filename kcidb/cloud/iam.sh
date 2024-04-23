# KCIDB cloud deployment - Identity and Access Management
#
if [ -z "${_IAM_SH+set}" ]; then
declare _IAM_SH=

. misc.sh

# Delete a project's IAM policy binding, if it exists
# Args: project member role
function iam_policy_binding_withdraw() {
    declare -r project="$1"; shift
    declare -r member="$1"; shift
    declare -r role="$1"; shift
    declare output
    if ! output=$(
            gcloud projects remove-iam-policy-binding \
                --quiet "$project" --member="$member" --role="$role" 2>&1
       ) && [[ $output != *\ not\ found!* ]]; then
        echo "$output" >&2
        false
    fi
}

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

# Disable a service account, if it exists
# Args: project name
function iam_service_account_disable() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    exists=$(iam_service_account_exists "$project" "$name")
    if "$exists"; then
        mute gcloud iam service-accounts disable \
            --quiet --project="$project" \
            "$name@$project.iam.gserviceaccount.com"
    fi
}

# Enable a service account, if it exists
# Args: project name
function iam_service_account_enable() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    exists=$(iam_service_account_exists "$project" "$name")
    if "$exists"; then
        mute gcloud iam service-accounts enable \
            --quiet --project="$project" \
            "$name@$project.iam.gserviceaccount.com"
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

# Deploy an IAM policy binding to a service account
# Args: project name role member
function iam_service_account_policy_binding_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare -r role="$1"; shift
    declare -r member="$1"; shift
    mute gcloud iam service-accounts add-iam-policy-binding \
        --quiet --project="$project" \
        --role="$role" --member="$member" "$name"
}

# Withdraw an IAM policy binding from a service account
# Args: project name role member
function iam_service_account_policy_binding_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare -r role="$1"; shift
    declare -r member="$1"; shift
    declare output
    if ! output=$(
            gcloud iam service-accounts remove-iam-policy-binding \
                --quiet --project="$project" \
                --role="$role" --member="$member" "$name" 2>&1
       ) && [[ $output != *@(\ not\ found!|NOT_FOUND)* ]]; then
        echo "$output" >&2
        false
    fi
}

# Check if an IAM role is deleted
# Args: project name
function iam_role_is_deleted() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    output=$(
        gcloud iam roles describe \
            --quiet --project="$project" \
            --format='value(deleted)' "$name" 2>&1
    )
    if [[ $output == True ]]; then
        echo "true"
    else
        echo "false"
    fi
}

# Undelete a deleted (but existing) role
# Args: project name
function iam_role_undelete() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    mute gcloud iam roles undelete --quiet --project="$project" "$name"
}

# Check if an IAM role exists
# Args: project name
function iam_role_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud iam roles describe \
                --quiet --project="$project" "$name" 2>&1
       ); then
        if [[ $output == True ]]; then
            echo "false"
        else
            echo "true"
        fi
    elif [[ $output == *@(NOT_FOUND|PERMISSION_DENIED|INVALID_ARGUMENT)* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Deploy an IAM role
# Args: project name
# Input: Role YAML
# (see https://cloud.google.com/iam/docs/reference/rest/v1/projects.roles)
function iam_role_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    declare deleted
    exists=$(iam_role_exists "$project" "$name")
    if "$exists"; then
        deleted=$(iam_role_is_deleted "$project" "$name")
        if "$deleted"; then
            iam_role_undelete "$project" "$name"
        fi
        mute gcloud iam roles update \
            --quiet --project="$project" --file=/dev/stdin "$name"
    else
        mute gcloud iam roles create \
            --quiet --project="$project" --file=/dev/stdin "$name"
    fi
}

# Delete a role, if it exists
# Args: project name
function iam_role_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    exists=$(iam_role_exists "$project" "$name")
    if "$exists"; then
        mute gcloud iam roles delete \
            --quiet --project="$project" \
            "$name"
    fi
}

# Deploy IAM
# Args: project grafana_service cost_upd cost_mon
function iam_deploy() {
    declare -r project="$1"; shift
    declare -r grafana_service="$1"; shift
    declare -r cost_upd="$1"; shift
    declare -r cost_mon="$1"; shift
    declare -r cost_mon_role="${cost_mon//-/_}"
    iam_service_account_deploy "$project" "$grafana_service"
    # Give Grafana access to Cloud SQL
    mute gcloud projects add-iam-policy-binding \
        --quiet --project="$project" "$project" \
        --role "roles/cloudsql.client" \
        --member \
        "serviceAccount:$grafana_service@$project.iam.gserviceaccount.com"

    # Deploy the cost updater service account
    iam_service_account_deploy "$project" "$cost_upd"

    # Deploy a cost monitor service account and corresponding role
    iam_role_deploy "$project" "$cost_mon_role" <<YAML_END
        # Prevent de-indent of the first line
        title: Cost Monitor
        # Permissions needed to shutdown the project
        includedPermissions:
          - cloudfunctions.functions.get
          - cloudfunctions.functions.delete
          - cloudfunctions.operations.get
          - run.services.getIamPolicy
          - run.services.setIamPolicy
          - pubsub.topics.getIamPolicy
          - pubsub.topics.setIamPolicy
YAML_END
    iam_service_account_deploy "$project" "$cost_mon"
    mute gcloud projects add-iam-policy-binding \
        --quiet --project="$project" "$project" \
        --role "projects/$project/roles/$cost_mon_role" \
        --member \
        "serviceAccount:$cost_mon@$project.iam.gserviceaccount.com"
}

# Withdraw IAM
# Args: project grafana_service cost_upd cost_mon
function iam_withdraw() {
    declare -r project="$1"; shift
    declare -r grafana_service="$1"; shift
    declare -r cost_upd="$1"; shift
    declare -r cost_mon="$1"; shift
    declare -r cost_mon_role="${cost_mon//-/_}"
    iam_service_account_withdraw "$project" "$cost_mon"
    iam_role_withdraw "$project" "$cost_mon_role"
    iam_service_account_withdraw "$project" "$cost_upd"
    iam_service_account_withdraw "$project" "$grafana_service"
}

fi # _IAM_SH
