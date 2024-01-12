# KCIDB cloud deployment - Pub/Sub management
#
if [ -z "${_PUBSUB_SH+set}" ]; then
declare _PUBSUB_SH=

. misc.sh

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
#       --updated-urls-topic=NAME
#       --smtp-topic=NAME
#       --smtp-subscription=NAME
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
                          updated_urls_topic \
                          smtp_topic smtp_subscription \
                          -- "$@")"
    eval "$params"
    pubsub_topic_deploy "$project" "${load_queue_trigger_topic}"

    pubsub_topic_deploy "$project" "${new_topic}"
    pubsub_subscription_deploy "$project" "${new_topic}" \
                               "${new_load_subscription}" \
                               --ack-deadline=600 \
                               --min-retry-delay=10s \
                               --max-retry-delay=600s

    pubsub_subscription_deploy "$project" "${new_topic}" \
                               "${new_debug_subscription}"

    pubsub_topic_deploy "$project" "${updated_topic}"
    pubsub_subscription_deploy "$project" "${updated_topic}" \
                               "${updated_debug_subscription}"
    pubsub_topic_deploy "$project" "${pick_notifications_trigger_topic}"
    pubsub_topic_deploy "$project" "${updated_urls_topic}"
    if [ -n "$smtp_topic" ]; then
        pubsub_topic_deploy "$project" "$smtp_topic"
        pubsub_subscription_deploy "$project" "$smtp_topic" \
                                   "$smtp_subscription"
    fi
}

# Withdraw from Pub/Sub
# Args: --project=NAME
#       --load-queue-trigger-topic=NAME
#       --pick-notifications-trigger-topic=NAME
#       --updated-urls-topic=NAME
#       --new-topic=NAME
#       --new-load-subscription=NAME
#       --new-debug-subscription=NAME
#       --updated-topic=NAME
#       --updated-debug-subscription=NAME
#       --smtp-topic=NAME
#       --smtp-subscription=NAME
function pubsub_withdraw() {
    declare params
    params="$(getopt_vars project \
                          load_queue_trigger_topic \
                          pick_notifications_trigger_topic \
                          updated_urls_topic \
                          new_topic \
                          new_load_subscription \
                          new_debug_subscription \
                          updated_topic \
                          updated_debug_subscription \
                          smtp_topic smtp_subscription \
                          -- "$@")"
    eval "$params"
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
}

fi # _PUBSUB_SH
