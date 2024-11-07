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

# Retrieve the URL of a service.
# Args: project name
# Output: The service URL.
function run_service_get_url() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    gcloud run services describe --project="$project" \
                                 --region="$RUN_REGION" \
                                 "$name" \
                                 --format="value(status.url)"
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
#       --cost-thresholds=JSON
#       --cost-mon-service=NAME
#       --cost-mon-image=URL
#       --cost-upd-service-account=EMAIL
#       --iss-ed-service=NAME
#       --iss-ed-image=URL
#       --new-topic=STRING
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
                          cost_thresholds \
                          cost_mon_service \
                          cost_mon_image \
                          cost_upd_service_account \
                          iss_ed_service \
                          iss_ed_image \
                          new_topic \
                          -- "$@")"
    eval "$params"
    declare iam_command

    # Deploy Grafana
    run_service_deploy "$project" <<YAML_END
        # Prevent de-indent of the first line
        apiVersion: serving.knative.dev/v1
        kind: Service
        metadata:
          name: $(yaml_quote "$grafana_service")
          labels:
            cloud.googleapis.com/location: $(yaml_quote "$RUN_REGION")
        spec:
          template:
            metadata:
              annotations:
                autoscaling.knative.dev/minScale: "1"
                autoscaling.knative.dev/maxScale: "4"
                run.googleapis.com/container-dependencies:
                  '{"grafana": ["cloud-sql-proxy"]}'
                run.googleapis.com/startup-cpu-boost: "true"
                run.googleapis.com/cpu-throttling: "false"
            spec:
              serviceAccountName:
                $(yaml_quote "$grafana_service@$project.iam.gserviceaccount.com")
              containerConcurrency: 512
              containers:
                - image: docker.io/grafana/grafana:11.3.0
                  name: grafana
                  ports:
                    - containerPort: 3000
                  env:
                    - name: GF_DATABASE_TYPE
                      value: postgres
                    - name: GF_DATABASE_HOST
                      value: 127.0.0.1:5432
                    - name: GF_DATABASE_USER
                      value: $(yaml_quote "$psql_grafana_user")
                    - name: GF_DATABASE_NAME
                      value: $(yaml_quote "$psql_grafana_database")
                    - name: GF_DATABASE_PASSWORD
                      valueFrom:
                        secretKeyRef:
                          name: $(yaml_quote "$(password_secret_get_name psql_grafana)")
                          key: latest
                    - name: GF_SERVER_ROOT_URL
                      value: $(yaml_quote "$grafana_url")
                    - name: GF_AUTH_ANONYMOUS_ORG_NAME
                      value: "KernelCI"
                    - name: GF_AUTH_ANONYMOUS_ORG_ROLE
                      value: "Viewer"
                    - name: GF_AUTH_ANONYMOUS_ENABLED
                      value: $(yaml_quote "$grafana_anonymous")
                  resources:
                    limits:
                      cpu: "1"
                      memory: "1G"
                - image: gcr.io/cloud-sql-connectors/cloud-sql-proxy:latest
                  name: cloud-sql-proxy
                  args:
                    - $(yaml_quote "$psql_conn?address=0.0.0.0&port=5432")
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

    # Deploy Cost Monitor
    run_service_deploy "$project" <<YAML_END
        # Prevent de-indent of the first line
        apiVersion: serving.knative.dev/v1
        kind: Service
        metadata:
          name: $(yaml_quote "$cost_mon_service")
          labels:
            cloud.googleapis.com/location: $(yaml_quote "$RUN_REGION")
        spec:
          template:
            metadata:
              annotations:
                autoscaling.knative.dev/minScale: "1"
                autoscaling.knative.dev/maxScale: "1"
            spec:
              serviceAccountName:
                $(yaml_quote \
                  "$cost_mon_service@$project.iam.gserviceaccount.com")
              containerConcurrency: 1
              timeoutSeconds: $((60*30))
              containers:
                - image: $(yaml_quote "$cost_mon_image:latest")
                  name: server
                  ports:
                    - containerPort: 8080
                  args:
                    - $(yaml_quote "$cost_thresholds")
                  resources:
                    limits:
                      cpu: "0.25"
                      memory: "256M"
YAML_END
    # Allow the cost updater to invoke the cost monitor
    run_iam_policy_binding_deploy \
        "$project" "$cost_mon_service" \
        "serviceAccount:$cost_upd_service_account" \
        roles/run.invoker

    # Deploy Issue Editor
    run_service_deploy "$project" <<YAML_END
        # Prevent de-indent of the first line
        apiVersion: serving.knative.dev/v1
        kind: Service
        metadata:
          name: $(yaml_quote "$iss_ed_service")
          labels:
            cloud.googleapis.com/location: $(yaml_quote "$RUN_REGION")
        spec:
          template:
            metadata:
              annotations:
                autoscaling.knative.dev/minScale: "0"
                autoscaling.knative.dev/maxScale: "4"
            spec:
              serviceAccountName:
                $(yaml_quote \
                  "$iss_ed_service@$project.iam.gserviceaccount.com")
              containerConcurrency: 1
              timeoutSeconds: 30
              containers:
                - image: $(yaml_quote "$iss_ed_image:latest")
                  name: server
                  ports:
                    - containerPort: 8080
                  resources:
                    limits:
                      cpu: "0.25"
                      memory: "256M"
                  env:
                    - name: KCIDB_PROJECT
                      value: $project
                    - name: KCIDB_NEW_TOPIC
                      value: $new_topic
YAML_END
}

# Shutdown Run services.
# Args: --project=ID
#       --grafana-service=NAME
function run_shutdown() {
    declare params
    params="$(getopt_vars project \
                          grafana_service \
                          -- "$@")"
    eval "$params"
    # Remove public access to Grafana
    run_iam_policy_binding_withdraw "$project" "$grafana_service" \
                                    allUsers roles/run.invoker
}

# Withdraw from Run.
# Args: --project=ID
#       --grafana-service=NAME
#       --cost-mon-service=NAME
#       --cost-upd-service-account=EMAIL
#       --iss-ed-service=NAME
function run_withdraw() {
    declare params
    params="$(getopt_vars project \
                          grafana_service \
                          cost_mon_service \
                          cost_upd_service_account \
                          iss_ed_service \
                          -- "$@")"
    eval "$params"
    # Withdraw Issue Editor
    run_service_withdraw "$project" "$iss_ed_service"
    # Withdraw Cost updater
    run_iam_policy_binding_withdraw \
        "$project" "$cost_mon_service" \
        "serviceAccount:$cost_upd_service_account" \
        roles/run.invoker
    run_service_withdraw "$project" "$cost_mon_service"
    # Withdraw Grafana
    run_iam_policy_binding_withdraw "$project" "$grafana_service" \
                                    allUsers roles/run.invoker
    run_service_withdraw "$project" "$grafana_service"
}

fi # _RUN_SH
