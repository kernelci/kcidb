# KCIDB cloud deployment - artifact registry management
#
if [ -z "${_ARTIFACTS_SH+set}" ]; then
declare _ARTIFACTS_SH=

. misc.sh

declare -r ARTIFACTS_REGION="us-central1"

# Check if an artifacts repository exists
# Args: project name
function artifacts_repo_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud artifacts repositories describe \
                --quiet --project="$project" --location="$ARTIFACTS_REGION" \
                "$name" 2>&1
       ); then
        echo "true"
    elif [[ $output == *@(NOT_FOUND|PERMISSION_DENIED)* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Deploy an artifacts repository, if not exists
# Args: project name [arg...]
function artifacts_repo_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    exists=$(artifacts_repo_exists "$project" "$name")
    if ! "$exists"; then
        mute gcloud artifacts repositories create \
            --project="$project" --location="$ARTIFACTS_REGION" \
            "$name" "$@"
    fi
}

# Delete an artifacts repository, if it exists
# Args: project name
function artifacts_repo_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists
    exists=$(artifacts_repo_exists "$project" "$name")
    if "$exists"; then
        mute gcloud artifacts repositories delete \
            --quiet --project="$project" --location="$ARTIFACTS_REGION" \
            "$name"
    fi
}

# Deploy all artifacts.
# Args: project repo cost_mon_image
function artifacts_deploy() {
    declare -r project="$1"; shift
    declare -r docker_repo="$1"; shift
    declare -r cost_mon_image="$1"; shift

    # Deploy the Docker repository
    artifacts_repo_deploy "$project" "$docker_repo" --repository-format=docker
    # Deploy the cost monitor docker image
    mute gcloud builds submit --project="$project" \
                              --region="$ARTIFACTS_REGION" \
                              --config /dev/stdin \
                              . <<YAML_END
        # Prevent de-indent of the first line
        steps:
          - name: 'gcr.io/cloud-builders/docker'
            args:
              - 'build'
              - '-t'
              - '$cost_mon_image'
              - '-f'
              - 'kcidb/cloud/cost-mon.Dockerfile'
              - '.'
        images:
          - '$cost_mon_image'
YAML_END
}

# Withdraw all artifacts.
# Args: project repo
function artifacts_withdraw() {
    declare -r project="$1"; shift
    declare -r docker_repo="$1"; shift
    # Withdraw the Docker repository, along with all the artifacts
    artifacts_repo_withdraw "$project" "$docker_repo"
}

fi # _ARTIFACTS_SH
