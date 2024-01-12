# KCIDB cloud deployment - submitter management
#
if [ -z "${_SUBMITTER_SH+set}" ]; then
declare _SUBMITTER_SH=

. pubsub.sh
. misc.sh

# Deploy permissions for a submitter
# Args: project new_topic submitter
function submitter_deploy() {
    declare -r project="$1"; shift
    declare -r new_topic="$1"; shift
    declare -r submitter="$1"; shift
    declare member
    declare role

    member="serviceAccount:$submitter@$project.iam.gserviceaccount.com"

    role="roles/pubsub.publisher"
    mute gcloud pubsub topics add-iam-policy-binding --project="$project" \
                                                     "$new_topic" \
                                                     --quiet \
                                                     --member="$member" \
                                                     --role="$role"
}

# Remove permissions for a submitter
# Args: project new_topic submitter
function submitter_withdraw() {
    declare -r project="$1"; shift
    declare -r new_topic="$1"; shift
    declare -r submitter="$1"; shift
    declare member
    member="serviceAccount:$submitter@$project.iam.gserviceaccount.com"

    iam_policy_binding_withdraw "$project" "$member" \
                                "roles/bigquery.dataViewer"
    iam_policy_binding_withdraw "$project" "$member" \
                                "roles/bigquery.jobUser"
    pubsub_topic_iam_policy_binding_withdraw "$project" "$new_topic" \
                                             "$member" \
                                             "roles/pubsub.publisher"
}

fi # _SUBMITTER_SH
