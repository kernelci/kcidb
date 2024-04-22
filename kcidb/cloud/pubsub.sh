# KCIDB cloud deployment - Pub/Sub management
#
if [ -z "${_PUBSUB_SH+set}" ]; then
declare _PUBSUB_SH=

. misc.sh
. iam.sh

# Check if a Pub/Sub topic exists
# Args: project name
# Output: "true" if the topic exists, "false" otherwise.
function pubsub_topic_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud pubsub topics describe --quiet --project="$project" \
                                          "$name" 2>&1
       ); then
        echo "true"
    elif [[ $output == *NOT_FOUND* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Create/update a Pub/Sub topic
# Args: project name [param_arg...]
function pubsub_topic_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    exists=$(pubsub_topic_exists "$project" "$name")
    if "$exists"; then
        if (($#)); then
            mute gcloud pubsub topics update --quiet --project="$project" \
                                             "$name" "$@"
        fi
    else
        mute gcloud pubsub topics create --quiet --project="$project" \
                                         "$name" "$@"
    fi
}

# Delete a Pub/Sub topic if it exists
# Args: project name
function pubsub_topic_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    exists=$(pubsub_topic_exists "$project" "$name")
    if "$exists"; then
        mute gcloud pubsub topics delete --quiet --project="$project" "$name"
    fi
}

# Delete a Pub/Sub topic's IAM policy binding, if it exists
# Args: project name member role
function pubsub_topic_iam_policy_binding_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare -r member="$1"; shift
    declare -r role="$1"; shift
    declare output
    if ! output=$(
            gcloud pubsub topics remove-iam-policy-binding --quiet "$name" \
                --project="$project" --member="$member" --role="$role" 2>&1
       ) && [[ $output != *@(\ not\ found!|NOT_FOUND)* ]]; then
        echo "$output" >&2
        false
    fi
}

# Check if a Pub/Sub subscription exists
# Args: project name
# Output: "true" if the subscription exists, "false" otherwise.
function pubsub_subscription_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud pubsub subscriptions describe --quiet \
                                                 --project="$project" \
                                                 "$name" 2>&1
       ); then
        echo "true"
    elif [[ $output == *NOT_FOUND* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Create/update a Pub/Sub subscription
# Args: project topic name [param_args...]
function pubsub_subscription_deploy() {
    declare -r project="$1"; shift
    declare -r topic="$1"; shift
    declare -r name="$1"; shift
    exists=$(pubsub_subscription_exists "$project" "$name")
    if "$exists"; then
        if (($#)); then
            mute gcloud pubsub subscriptions update --quiet \
                                                    --project="$project" \
                                                    "$name" "$@"
        fi
    else
        mute gcloud pubsub subscriptions create --quiet \
                                                --project="$project" \
                                                --topic="$topic" \
                                                "$name" "$@"
    fi
}

# Delete a Pub/Sub subscription if it exists
# Args: project name
function pubsub_subscription_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    exists=$(pubsub_subscription_exists "$project" "$name")
    if "$exists"; then
        mute gcloud pubsub subscriptions delete --quiet \
                                                --project="$project" \
                                                "$name"
    fi
}

# Deploy to Pub/Sub
# Args: --project=NAME
#       --load-queue-trigger-topic=NAME
#       --new-topic=NAME
#       --new-load-subscription=NAME
#       --new-debug-subscription=NAME
#       --updated-topic=NAME
#       --updated-debug-subscription=NAME
#       --pick-notifications-trigger-topic=NAME
#       --purge-db-trigger-topic=NAME
#       --updated-urls-topic=NAME
#       --smtp-topic=NAME
#       --smtp-subscription=NAME
#       --cost-topic=NAME
#       --cost-upd-service-account=NAME
#       --cost-mon-service=NAME
function pubsub_deploy() {
    declare params
    params="$(getopt_vars project \
                          load_queue_trigger_topic \
                          new_topic \
                          new_load_subscription \
                          new_debug_subscription \
                          updated_topic \
                          updated_debug_subscription \
                          pick_notifications_trigger_topic \
                          purge_db_trigger_topic \
                          updated_urls_topic \
                          smtp_topic smtp_subscription \
                          cost_topic \
                          cost_upd_service_account \
                          cost_mon_service \
                          -- "$@")"
    eval "$params"
    declare project_number
    project_number=$(gcloud projects describe "$project" \
                                              --format='value(projectNumber)')
    # Pub/Sub service account
    declare service_account="service-$project_number"
    service_account+="@gcp-sa-pubsub.iam.gserviceaccount.com"

    pubsub_topic_deploy "$project" "${load_queue_trigger_topic}"

    pubsub_topic_deploy "$project" "${new_topic}"
    pubsub_subscription_deploy "$project" "${new_topic}" \
                               "${new_load_subscription}" \
                               --ack-deadline=600 \
                               --min-retry-delay=10s \
                               --max-retry-delay=600s

    pubsub_subscription_deploy "$project" "${new_topic}" \
                               "${new_debug_subscription}" \
                               --message-retention-duration=12h

    pubsub_topic_deploy "$project" "${updated_topic}"
    pubsub_subscription_deploy "$project" "${updated_topic}" \
                               "${updated_debug_subscription}" \
                               --message-retention-duration=12h
    pubsub_topic_deploy "$project" "${pick_notifications_trigger_topic}"
    pubsub_topic_deploy "$project" "${purge_db_trigger_topic}"
    pubsub_topic_deploy "$project" "${updated_urls_topic}"
    if [ -n "$smtp_topic" ]; then
        pubsub_topic_deploy "$project" "$smtp_topic"
        pubsub_subscription_deploy "$project" "$smtp_topic" \
                                   "$smtp_subscription" \
                                   --message-retention-duration=12h
    fi

    # Allow the Pub/Sub service account to create cost updater tokens
    # so it can push to cost updater container subscription
    iam_service_account_policy_binding_deploy \
        "$project" "$cost_upd_service_account" \
        "roles/iam.serviceAccountTokenCreator" \
        "serviceAccount:$service_account"

    # Get the cost monitor trigger HTTP URL
    declare cost_mon_service_url
    cost_mon_service_url=$(
        run_service_get_url "$project" "$cost_mon_service"
    )
    pubsub_topic_deploy "$project" "$cost_topic"
    pubsub_subscription_deploy \
        "$project" "$cost_topic" \
        "${cost_topic}_mon" \
        --message-retention-duration=12h \
        --push-endpoint="$cost_mon_service_url" \
        --push-auth-service-account="$cost_upd_service_account"
}

# Withdraw from Pub/Sub
# Args: --project=NAME
#       --load-queue-trigger-topic=NAME
#       --pick-notifications-trigger-topic=NAME
#       --purge-db-trigger-topic=NAME
#       --updated-urls-topic=NAME
#       --new-topic=NAME
#       --new-load-subscription=NAME
#       --new-debug-subscription=NAME
#       --updated-topic=NAME
#       --updated-debug-subscription=NAME
#       --smtp-topic=NAME
#       --smtp-subscription=NAME
#       --cost-topic=NAME
#       --cost-upd-service-account=NAME
function pubsub_withdraw() {
    declare params
    params="$(getopt_vars project \
                          load_queue_trigger_topic \
                          pick_notifications_trigger_topic \
                          purge_db_trigger_topic \
                          updated_urls_topic \
                          new_topic \
                          new_load_subscription \
                          new_debug_subscription \
                          updated_topic \
                          updated_debug_subscription \
                          smtp_topic smtp_subscription \
                          cost_topic \
                          cost_upd_service_account \
                          -- "$@")"
    eval "$params"
    declare project_number
    project_number=$(gcloud projects describe "$project" \
                                              --format='value(projectNumber)')
    # Pub/Sub service account
    declare service_account="service-$project_number"
    service_account+="@gcp-sa-pubsub.iam.gserviceaccount.com"

    if [ -n "$smtp_topic" ]; then
        pubsub_subscription_withdraw "$project" "$smtp_subscription"
        pubsub_topic_withdraw "$project" "$smtp_topic"
    fi
    pubsub_subscription_withdraw "$project" "$updated_debug_subscription"
    pubsub_topic_withdraw "$project" "$updated_topic"
    pubsub_subscription_withdraw "$project" "$new_debug_subscription"
    pubsub_subscription_withdraw "$project" "$new_load_subscription"
    pubsub_topic_withdraw "$project" "$new_topic"
    pubsub_topic_withdraw "$project" "$load_queue_trigger_topic"
    pubsub_topic_withdraw "$project" "$pick_notifications_trigger_topic"
    pubsub_topic_withdraw "$project" "$updated_urls_topic"
    pubsub_topic_withdraw "$project" "$purge_db_trigger_topic"
    pubsub_subscription_withdraw "$project" "${cost_topic}_mon"
    pubsub_topic_withdraw "$project" "$cost_topic"

    # Withdraw the permission for the Pub/Sub service account to create cost
    # updater tokens
    iam_service_account_policy_binding_withdraw \
        "$project" "$cost_upd_service_account" \
        "roles/iam.serviceAccountTokenCreator" \
        "serviceAccount:$service_account"
}

fi # _PUBSUB_SH
