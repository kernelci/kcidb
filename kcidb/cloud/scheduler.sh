# KCIDB cloud deployment - Cloud Scheduler management
#
if [ -z "${_SCHEDULER_SH+set}" ]; then
declare _SCHEDULER_SH=

. misc.sh

# Check if a scheduler job exists
# Args: project name
# Output: "true" if the subscription exists, "false" otherwise.
function scheduler_job_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud scheduler jobs describe --quiet --project="$project" \
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

# Deploy a pubsub scheduler job
# Args: project name topic schedule message_body
function scheduler_job_pubsub_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare -r topic="$1"; shift
    declare -r schedule="$1"; shift
    declare -r message_body="$1"; shift
    declare -r -a args=(
        "${name}"
        --quiet
        --project="$project"
        --topic="$topic"
        --schedule="$schedule"
        --message-body="$message_body"
    )
    exists=$(scheduler_job_exists "$project" "$name")
    if "$exists"; then
        mute gcloud scheduler jobs update pubsub "${args[@]}"
    else
        mute gcloud scheduler jobs create pubsub "${args[@]}"
    fi
}

# Delete a scheduler job if it exists
# Args: project name
function scheduler_job_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    exists=$(scheduler_job_exists "$project" "$name")
    if "$exists"; then
        mute gcloud scheduler jobs delete \
            --quiet --project="$project" "$name"
    fi
}

# Deploy to scheduler
# Args: --project=NAME
#       --prefix=STRING
#       --load-queue-trigger-topic=NAME
#       --pick-notifications-trigger-topic=NAME
#       --purge-db-trigger-topic=NAME
function scheduler_deploy() {
    declare params
    params="$(getopt_vars project \
                          prefix \
                          load_queue_trigger_topic \
                          pick_notifications_trigger_topic \
                          purge_db_trigger_topic \
                          -- "$@")"
    eval "$params"
    # Deploy the jobs
    scheduler_job_pubsub_deploy "$project" "${prefix}load_queue_trigger" \
                                "$load_queue_trigger_topic" '* * * * *' '{}'
    scheduler_job_pubsub_deploy "$project" "${prefix}pick_notifications_trigger" \
                                "$pick_notifications_trigger_topic" \
                                 '*/10 * * * *' '{}'
    scheduler_job_pubsub_deploy \
        "$project" "${prefix}purge_op_db_trigger" \
        "$purge_db_trigger_topic" '0 6 * * MON' \
        '{"database": "op", "timedelta": {"delta": {"months": 6}}}'
    scheduler_job_pubsub_deploy \
        "$project" "${prefix}purge_sm_db_trigger" \
        "$purge_db_trigger_topic" '0 7 * * *' \
        '{"database": "sm", "timedelta": {"delta": {"weeks": 4}}}'
}

# Withdraw from the scheduler
# Args: project prefix
function scheduler_withdraw() {
    declare -r project="$1"; shift
    declare -r prefix="$1"; shift
    scheduler_job_withdraw "$project" "${prefix}load_queue_trigger"
    scheduler_job_withdraw "$project" "${prefix}pick_notifications_trigger"
    scheduler_job_withdraw "$project" "${prefix}purge_op_db_trigger"
}

fi # _SCHEDULER_SH
