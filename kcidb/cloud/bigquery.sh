# KCIDB cloud deployment - BigQuery
#
if [ -z "${_BIGQUERY_SH+set}" ]; then
declare _BIGQUERY_SH=

. misc.sh

# Deploy to BigQuery
# Do not initialize datasets that are prepended with the hash sign ('#').
# Args: project dataset...
function bigquery_deploy() {
    declare -r project="$1"; shift
    declare dataset
    declare init
    for dataset in "$@"; do
        # Handle possible leading hash sign
        if [[ $dataset == \#* ]]; then
            dataset="${dataset:1}"
            init=false
        else
            init=true
        fi
        mute bq mk --project_id="$project" --force "$dataset"
        if "$init"; then
            kcidb-db-init -lDEBUG -d "bigquery:${project}.${dataset}" \
                          --ignore-initialized
        fi
    done
}

# Withdraw from BigQuery
# Cleanup all datasets, even those prepended with the hash sign ('#').
# Args: project dataset...
function bigquery_withdraw() {
    declare -r project="$1"; shift
    declare dataset
    for dataset in "$@"; do
        # Ignore possible leading hash sign
        dataset="${dataset###}"
        kcidb-db-cleanup -lDEBUG -d "bigquery:${project}.${dataset}" \
                         --ignore-not-initialized \
                         --ignore-not-found
        mute bq rm --project_id="$project" --force "$dataset"
    done
}

fi # _BIGQUERY_SH
