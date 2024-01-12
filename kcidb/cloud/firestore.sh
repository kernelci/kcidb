# KCIDB cloud deployment - Firestore management
#
if [ -z "${_FIRESTORE_SH+set}" ]; then
declare _FIRESTORE_SH=

. misc.sh

# Check if the native Firestore database exists
# Args: project
function firestore_exists() {
    declare -r project="$1"; shift
    declare output
    if output=$(
            gcloud firestore databases describe \
                --quiet --project="$project" 2>&1
       ); then
        echo "true"
    elif [[ $output == *database\ \'\(default\)\'\ does\ not\ exist.* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Create the native Firestore database.
# Args: project
function firestore_create() {
    declare -r project="$1"; shift
    # Create the native database (in the same region as the App Engine app)
    mute gcloud firestore databases create --quiet \
                                           --project="$project" \
                                           --type=firestore-native \
                                           --location=nam5
}

# Deploy to Firestore.
# Args: project
function firestore_deploy() {
    declare -r project="$1"; shift
    declare exists
    exists=$(firestore_exists "$project")
    if ! "$exists"; then
        firestore_create "$project"
    fi
}

# Withdraw from Firestore.
# Args: project spool_collection_path
function firestore_withdraw() {
    declare -r project="$1"; shift
    declare -r spool_collection_path="$1"; shift
    kcidb-monitor-spool-wipe -p "$project" -c "$spool_collection_path"
}

fi # _FIRESTORE_SH
