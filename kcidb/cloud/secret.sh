# KCIDB cloud deployment - secret management
#
if [ -z "${_SECRET_SH+set}" ]; then
declare _SECRET_SH=

. misc.sh

# Check if a secret exists
# Args: project name
# Output: "true" if the secret exists, "false" otherwise.
function secret_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud secrets describe --quiet --project="$project" "$name" 2>&1
       ); then
        echo "true"
    elif [[ $output == *NOT_FOUND* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Deploy a secret
# Args: project name
# Input: value
function secret_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare exists

    exists=$(secret_exists "$project" "$name")
    if "$exists"; then
        mute gcloud secrets versions add --quiet --project="$project" \
                                         "$name" \
                                         --data-file=-
    else
        mute gcloud secrets create --quiet --project="$project" "$name" \
                                   --replication-policy automatic \
                                   --data-file=-
    fi
}

# Retrieve a secret
# Args: project name
# Output: value
function secret_get() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    gcloud secrets versions access latest \
        --quiet --project="$project" --secret="$name" \
        --format='get(payload.data)' | tr '_-' '/+' | base64 -d
}

# Delete a secret if it exists
# Args: project name
function secret_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    exists=$(secret_exists "$project" "$name")
    if "$exists"; then
        mute gcloud secrets delete --quiet --project="$project" "$name"
    fi
}

fi # _SECRET_SH
