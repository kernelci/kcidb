# KCIDB cloud deployment - Cloud Function management
#
if [ -z "${_FUNCTION_SH+set}" ]; then
declare _FUNCTION_SH=

. sections.sh
. misc.sh

# The region used to host our Cloud Functions
declare -r FUNCTION_REGION="us-central1"

# Add a function's IAM policy binding
# Args: project prefix name member role
function function_iam_policy_binding_deploy() {
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    declare -r member="$1"; shift
    declare -r role="$1"; shift
    mute gcloud functions add-iam-policy-binding \
        --quiet --project="$project" \
        "${prefix}${name}" \
        --region="$FUNCTION_REGION" \
        --member="$member" \
        --role="$role"
}

# Delete a function's IAM policy binding, if it exists
# Args: project prefix name member role
function function_iam_policy_binding_withdraw() {
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    declare -r member="$1"; shift
    declare -r role="$1"; shift
    declare output
    if ! output=$(
            gcloud functions remove-iam-policy-binding \
                --quiet --project="$project" \
                "${prefix}${name}" \
                --region="$FUNCTION_REGION" \
                --member="$member" \
                --role="$role" 2>&1
       ) && [[ $output != *\ not\ found!* ]]; then
        echo "$output" >&2
        false
    fi
}

# Deploy a Cloud Function regardless if its section is enabled or not.
# Args: source project prefix name auth [param_arg...]
# Where "auth" is either "true" or "false" for an authenticated and
# unauthenticated deployment respectively.
function function_deploy_unconditional() {
    declare -r source="$1"; shift
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    declare -r auth="$1"; shift
    declare iam_action

    assert test "$auth" = "true" -o "$auth" = "false"

    # TODO Upgrade to gen2
    mute gcloud functions deploy --quiet --project="$project" \
                                 --region="$FUNCTION_REGION" \
                                 --docker-registry=artifact-registry \
                                 --runtime python39 \
                                 --no-gen2 \
                                 --source "$source" "${prefix}${name}" \
                                 --entry-point "kcidb_${name}" \
                                 "$@"

    # Work around broken --allow-unauthenticated option
    if "$auth"; then
        iam_action="withdraw"
    else
        iam_action="deploy"
    fi
    "function_iam_policy_binding_$iam_action" \
        "$project" "$prefix" "$name" "allUsers" "roles/cloudfunctions.invoker"
}

# Deploy a Cloud Function
# Args: sections source project prefix name auth [param_arg...]
# Where "auth" is either "true" or "false" for an authenticated and
# unauthenticated deployment respectively.
function function_deploy() {
    declare -r sections="$1"; shift
    declare -r source="$1"; shift
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    declare -r name="$1"; shift
    declare -r auth="$1"; shift

    assert test "$auth" = "true" -o "$auth" = "false"

    # TODO Upgrade to gen2
    sections_run_explicit "$sections" \
        "functions.$name" deploy \
        function_deploy_unconditional "$source" "$project" "$prefix" \
                                      "$name" "$auth" "$@"
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
