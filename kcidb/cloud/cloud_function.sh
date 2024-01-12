# KCIDB cloud deployment - Cloud Function management
#
if [ -z "${_CLOUD_FUNCTION_SH+set}" ]; then
declare _CLOUD_FUNCTION_SH=

. sections.sh
. misc.sh

# The region used to host our Cloud Functions
declare -r CLOUD_FUNCTION_REGION="us-central1"

# Deploy a Cloud Function
# Args: sections source project prefix name [param_arg...]
function cloud_function_deploy() {
    declare -r sections="$1"; shift
    declare -r source="$1"; shift
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    sections_run_explicit "$sections" \
        "cloud_functions.$name" deploy \
        mute gcloud functions deploy --quiet --project="$project" \
                                     --region="$CLOUD_FUNCTION_REGION" \
                                     --docker-registry=artifact-registry \
                                     --runtime python39 \
                                     --source "$source" "${prefix}${name}" \
                                     --entry-point "kcidb_${name}" \
                                     "$@"
}

# Delete a Cloud Function (without complaining it doesn't exist).
# Accepts the arguments of "gcloud functions delete".
function cloud_function_delete()
{
    declare output
    if ! output=$(gcloud functions delete "$@" 2>&1) &&
       [[ $output != *\ status=\[404\]* ]]; then
        echo "$output" >&2
        false
    fi
}

# Delete a Cloud Function if it exists
# Args: sections project prefix name
function cloud_function_withdraw() {
    declare -r sections="$1"; shift
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    sections_run_explicit "$sections" \
        "cloud_functions.$name" withdraw \
        cloud_function_delete --quiet --project="$project" "${prefix}${name}"
}

fi # _CLOUD_FUNCTION_SH
