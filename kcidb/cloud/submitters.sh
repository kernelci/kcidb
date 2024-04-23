# KCIDB cloud deployment - submitters
#
if [ -z "${_SUBMITTERS_SH+set}" ]; then
declare _SUBMITTERS_SH=

. submitter.sh
. misc.sh

# Deploy submitter permissions
# Args: project new_topic [submitter...]
function submitters_deploy() {
    declare -r project="$1"; shift
    declare -r new_topic="$1"; shift
    declare -r -a submitters=("$@")
    for submitter in "${submitters[@]}"; do
        submitter_deploy "$project" "$new_topic" "$submitter"
    done
}

# Withdraw submitter permissions
# Args: project new_topic [submitter...]
function submitters_withdraw() {
    declare -r project="$1"; shift
    declare -r new_topic="$1"; shift
    declare -r -a submitters=("$@")
    for submitter in "${submitters[@]}"; do
        submitter_withdraw "$project" "$new_topic" "$submitter"
    done
}

# Shutdown submitters
# Args: project new_topic [submitter...]
function submitters_shutdown() {
    submitters_withdraw "$@"
    # Deploying will add permissions again
}

fi # _SUBMITTERS_SH
