# KCIDB cloud deployment - storage
#
if [ -z "${_STORAGE_SH+set}" ]; then
declare _STORAGE_SH=

. misc.sh

# Deploy a Google Cloud Storage Bucket
# Args: project bucket
function storage_deploy() {
    declare -r project="$1"; shift
    declare -r bucket="$1"; shift
    # Check if the bucket exists
    if ! TMPDIR="$TMPDIR_ORIG" gsutil ls "gs://$bucket" &>/dev/null; then
        # Create the bucket if it doesn't exist
        TMPDIR="$TMPDIR_ORIG" gsutil -q mb -p "$project" -c STANDARD \
            -l "us-central1" -b on "gs://$bucket"
    fi
    TMPDIR="$TMPDIR_ORIG" gsutil -q iam ch allUsers:objectViewer "gs://$bucket/"
}

# Remove a Google Cloud Storage Bucket and its contents
# Args: bucket
function storage_withdraw() {
    declare -r bucket="$1"; shift
    # Check if the bucket exists
    if TMPDIR="$TMPDIR_ORIG" gsutil ls "gs://$bucket" &>/dev/null; then
        # Remove the bucket and its contents if it exists
        TMPDIR="$TMPDIR_ORIG" gsutil -q -m rm -r "gs://$bucket"
    fi
}

fi # _STORAGE_SH
