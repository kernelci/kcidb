# KCIDB cloud deployment - sections
#
if [ -z "${_SECTIONS_SH+set}" ]; then
declare _SECTIONS_SH=

. aterr.sh
. misc.sh

# Sections of the installation
declare -A -r SECTIONS=(
    ["iam"]="Identity and Access Management"
    ["bigquery"]="BigQuery dataset"
    ["psql"]="PostgreSQL database"
    ["pubsub"]="Pub/Sub topics and subscriptions"
    ["secrets"]="Secrets"
    ["firestore"]="Firestore database"
    ["storage"]="Google cloud storage"
    ["functions.purge_db"]="Cloud Functions: kcidb_purge_db()"
    ["functions.archive"]="Cloud Functions: kcidb_archive()"
    ["functions.pick_notifications"]="Cloud Functions: kcidb_pick_notifications()"
    ["functions.send_notification"]="Cloud Functions: kcidb_send_notification()"
    ["functions.spool_notifications"]="Cloud Functions: kcidb_spool_notifications()"
    ["functions.cache_redirect"]="Cloud Functions: kcidb_cache_redirect()"
    ["functions.cache_urls"]="Cloud Functions: kcidb_cache_urls()"
    ["functions.load_queue"]="Cloud Functions: kcidb_load_queue()"
    ["scheduler"]="Scheduler jobs"
    ["submitters"]="Submitter permissions"
    ["artifacts"]="Artifact repository"
    ["run"]="Cloud Run"
)
# Maximum length of a section name
declare SECTIONS_NAME_LEN_MAX=0
# Maximum length of a section description
declare SECTIONS_DESCRIPTION_LEN_MAX=0
# Calculate maximum lengths
declare SECTIONS_NAME
for SECTIONS_NAME in "${!SECTIONS[@]}"; do
    if ((${#SECTIONS_NAME} > SECTIONS_NAME_LEN_MAX)); then
        SECTIONS_NAME_LEN_MAX="${#SECTIONS_NAME}"
    fi
    if ((${#SECTIONS[$SECTIONS_NAME]} > SECTIONS_DESCRIPTION_LEN_MAX)); then
        SECTIONS_DESCRIPTION_LEN_MAX="${#SECTIONS[$SECTIONS_NAME]}"
    fi
done
unset SECTIONS_NAME
declare -r SECTIONS_NAME_LEN_MAX
declare -r SECTIONS_DESCRIPTION_LEN_MAX

# Execute an operation on a section of installation, if its name matches a
# glob. The section name is passed as an explicit argument. The operation is
# also passed explicitly as the operation verb, and the corresponding command
# to execute.
#
# Args: glob name verb command [arg...]
function sections_run_explicit() {
    declare -r glob="$1"; shift
    declare -r name="$1"; shift
    declare -r verb="$1"; shift
    declare -r command="$1"; shift
    declare action

    if [ "$verb" == "shutdown" ]; then
        action="shutting down"
    else
        action="${verb}ing"
    fi

    if ! [[ -v SECTIONS[$name] ]]; then
        echo "Unknown section name ${name@Q}" >&2
        exit 1
    fi

    if [[ $name != $glob ]]; then
        return
    fi

    verbose printf "%s %-${SECTIONS_DESCRIPTION_LEN_MAX}s [%s]\\n" \
                   "${action^}" "${SECTIONS[$name]}" "$name"
    aterr_push "echo Failed ${action@Q} ${SECTIONS[$name]@Q} '['${name@Q}']'"
    "${command}" "$@"
    aterr_pop
}

# Execute an operation on a section of installation, if its name matches a
# glob. The operation is defined as a command with arguments. The command name
# must consist of the (multi-word) section name and the single-word operation
# verb, with all words separated by underscores.
#
# Args: glob command [arg...]
function sections_run() {
    declare -r glob="$1"; shift
    declare -r command="$1"; shift
    declare -r name="${command%_*}"
    declare -r verb="${command##*_}"

    if [ -z "$name" ]; then
        echo "No section name found in command name ${command@Q}" >&2
        exit 1
    fi
    if [ -z "$verb" ]; then
        echo "No operation verb found in command name ${command@Q}" >&2
        exit 1
    fi

    sections_run_explicit "$glob" "$name" "$verb" "$command" "$@"
}

fi # _SECTIONS_SH
