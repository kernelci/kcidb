# KCIDB cloud deployment - Cloud Run management
#
if [ -z "${_RUN_SH+set}" ]; then
declare _RUN_SH=

. misc.sh
. password.sh

declare -r RUN_REGION="us-central1"

# Deploy a Cloud Run service IAM policy binding
# Args: project service member role
function run_iam_policy_binding_deploy() {
    declare -r project="$1"; shift
    declare -r service="$1"; shift
    declare -r member="$1"; shift
    declare -r role="$1"; shift
    mute gcloud run services add-iam-policy-binding \
        --quiet --project "$project" --region="$RUN_REGION" \
        "$service" --member="$member" --role="$role"
}

# Delete a Cloud Run service IAM policy binding, if it exists
# Args: project service member role
function run_iam_policy_binding_withdraw() {
    declare -r project="$1"; shift
    declare -r service="$1"; shift
    declare -r member="$1"; shift
    declare -r role="$1"; shift
    declare output
    if ! output=$(
            gcloud run services remove-iam-policy-binding \
                --quiet --project "$project" --region="$RUN_REGION" \
                "$service" --member="$member" --role="$role" 2>&1
       ) && [[ $output != *\ not\ found!* ]]; then
        echo "$output" >&2
        false
    fi
}

# Deploy a service to Run.
# Args: project
# Input: Service YAML
function run_service_deploy() {
    declare -r project="$1"; shift
    mute gcloud run services replace --project="$project" --quiet -
}

# Delete a service from Run, if it exists.
# Args: project name
function run_service_withdraw() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if ! output=$(
        gcloud run services delete \
            --quiet --project "$project" --region="$RUN_REGION" "$name" 2>&1
    ) && [[ $output != *\ could\ not\ be\ found.* ]]; then
        echo "$output" >&2
        false
    fi
}

# Deploy to Run.
# Args: --project=ID
#       --grafana-service=NAME
#       --grafana-url=URL
#       --grafana-public=true|false
#       --grafana-anonymous=true|false
#       --psql-conn=STRING
#       --psql-grafana-user=NAME
#       --psql-grafana-database=NAME
function run_deploy() {
    declare params
    params="$(getopt_vars project \
                          grafana_service \
                          grafana_url \
                          grafana_public \
                          grafana_anonymous \
                          psql_conn \
                          psql_grafana_user \
                          psql_grafana_database \
                          -- "$@")"
    eval "$params"
    declare iam_command

    # Deploy Grafana
    run_service_deploy "$project" <<YAML_END
        # Prevent de-indent of the first line
        apiVersion: serving.knative.dev/v1
        kind: Service
        metadata:
          name: ${grafana_service@Q}
          labels:
            cloud.googleapis.com/location: ${RUN_REGION@Q}
        spec:
          template:
            metadata:
              annotations:
                autoscaling.knative.dev/minScale: "0"
                autoscaling.knative.dev/maxScale: "4"
                run.googleapis.com/container-dependencies:
                  '{"grafana": ["cloud-sql-proxy"]}'
            spec:
              serviceAccountName:
                "$grafana_service@$project.iam.gserviceaccount.com"
              containerConcurrency: 256
              containers:
                - image: docker.io/grafana/grafana:6.6.0
                  name: grafana
                  ports:
                    - containerPort: 3000
                  env:
                    - name: GF_DATABASE_TYPE
                      value: postgres
                    - name: GF_DATABASE_HOST
                      value: 127.0.0.1:5432
                    - name: GF_DATABASE_USER
                      value: ${psql_grafana_user@Q}
                    - name: GF_DATABASE_NAME
                      value: ${psql_grafana_database@Q}
                    - name: GF_DATABASE_PASSWORD
                      valueFrom:
                        secretKeyRef:
                          name: $(password_secret_get_name psql_grafana)
                          key: latest
                    - name: GF_INSTALL_PLUGINS
                      value: "\\
                        doitintl-bigquery-datasource,\\
                        https://github.com/kernelci/\\
                          grafana-singlevalue-panel/\\
                          releases/download/2.0.0/\\
                          grafana-singlevalue-panel-2.0.0.zip;\\
                          grafana-singlevalue-panel\\
                      "
                    - name: GF_SERVER_ROOT_URL
                      value: "${grafana_url}"
                    - name: GF_AUTH_ANONYMOUS_ORG_NAME
                      value: "KernelCI"
                    - name: GF_AUTH_ANONYMOUS_ORG_ROLE
                      value: "Viewer"
                    - name: GF_AUTH_ANONYMOUS_ENABLED
                      value: "${grafana_anonymous}"
                  resources:
                    limits:
                      cpu: "1"
                      memory: "1G"
                - image: gcr.io/cloud-sql-connectors/cloud-sql-proxy:latest
                  name: cloud-sql-proxy
                  args:
                    - "$psql_conn?address=0.0.0.0&port=5432"
                    - --health-check
                    - --http-address=0.0.0.0
                    - --http-port=9090
                  startupProbe:
                    httpGet:
                      path: /readiness
                      port: 9090
                  resources:
                    limits:
                      cpu: "0.5"
                      memory: "512M"
YAML_END

    # Update IAM policy for public access to Grafana
    if "$grafana_public"; then
        iam_command=run_iam_policy_binding_deploy
    else
        iam_command=run_iam_policy_binding_withdraw
    fi
    "$iam_command" "$project" "$grafana_service" allUsers roles/run.invoker
}

# Withdraw from Run.
# Args: --project=ID
#       --grafana-service=NAME
function run_withdraw() {
    declare params
    params="$(getopt_vars project \
                          grafana_service \
                          -- "$@")"
    eval "$params"
    # Withdraw Grafana
    run_iam_policy_binding_withdraw "$project" "$grafana_service" \
                                    allUsers roles/run.invoker
    run_service_withdraw "$project" "$grafana_service"
}

fi # _RUN_SH
