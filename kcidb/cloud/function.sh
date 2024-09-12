# KCIDB cloud deployment - Cloud Function management
#
if [ -z "${_FUNCTION_SH+set}" ]; then
declare _FUNCTION_SH=

. sections.sh
. misc.sh

# The region used to host our Cloud Functions
declare -r FUNCTION_REGION="us-central1"

# Deploy a Cloud Function
# Args: sections source project prefix name [param_arg...]
function function_deploy() {
    declare -r sections="$1"; shift
    declare -r source="$1"; shift
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    # TODO Upgrade to gen2
    sections_run_explicit "$sections" \
        "functions.$name" deploy \
        mute gcloud functions deploy --quiet --project="$project" \
                                     --region="$FUNCTION_REGION" \
                                     --docker-registry=artifact-registry \
                                     --runtime python39 \
                                     --no-gen2 \
                                     --source "$source" "${prefix}${name}" \
                                     --entry-point "kcidb_${name}" \
                                     "$@"
}

# Delete a Cloud Function (without complaining it doesn't exist).
# Accepts the arguments of "gcloud functions delete".
function function_delete()
{
    declare output
    if ! output=$(gcloud functions delete "$@" 2>&1) &&
       [[ $output != *\ status=\[404\]* ]]; then
        echo "$output" >&2
        false
    fi
}

# Shutdown a Cloud Function if it exists
# Args: sections project prefix name
function function_shutdown() {
    declare -r sections="$1"; shift
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    sections_run_explicit "$sections" \
        "functions.$name" shutdown \
        function_delete --quiet --project="$project" "${prefix}${name}"
}

# Delete a Cloud Function if it exists
# Args: sections project prefix name
function function_withdraw() {
    declare -r sections="$1"; shift
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    sections_run_explicit "$sections" \
        "functions.$name" withdraw \
        function_delete --quiet --project="$project" "${prefix}${name}"
}

fi # _FUNCTION_SH
