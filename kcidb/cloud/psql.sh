# KCIDB cloud deployment - PostgreSQL management
#
if [ -z "${_PSQL_SH+set}" ]; then
declare _PSQL_SH=

. password.sh
. atexit.sh
. misc.sh

# Location of the PostgreSQL proxy binary we (could) download
declare PSQL_PROXY_BINARY="$TMPDIR/cloud_sql_proxy"

# Location of the PostgreSQL proxy socket directory
declare PSQL_PROXY_DIR="$TMPDIR/cloud_sql_sockets"

# File containing the PID of the shell executing PostgreSQL proxy, if started
declare PSQL_PROXY_SHELL_PID_FILE="$TMPDIR/cloud_sql_proxy.pid"

# The .pgpass file for the command running through PostgreSQL proxy
declare PSQL_PROXY_PGPASS="$TMPDIR/cloud_sql_proxy.pgpass"

# Cleanup PostgreSQL after the script
function _psql_cleanup() {
    # Kill the cloud_sql_proxy, if started
    if [ -e "$PSQL_PROXY_SHELL_PID_FILE" ]; then
        declare pid
        pid=$(< "$PSQL_PROXY_SHELL_PID_FILE")
        pkill -P "$pid" 2>/dev/null || true
        rm -f "$PSQL_PROXY_SHELL_PID_FILE"
    fi
}

# Cleanup PostgreSQL setup on exit
atexit_push _psql_cleanup

# The name of the Cloud SQL instance we're creating/using
# Specified statically as instance names have 7-day recycling period
declare -r PSQL_INSTANCE="postgresql"

# The region used to host our PostgreSQL instance
declare -r PSQL_INSTANCE_REGION="us-central1"

# The tier used for the automatically-created PostgreSQL instance
declare -r PSQL_INSTANCE_TIER="db-f1-micro"

# The name of the PostgreSQL viewer user. Granted read-only permissions for
# all KCIDB databases to make it easier to switch the queried database in UI.
declare -r PSQL_VIEWER="kcidb_viewer"

# Check if a PostgreSQL instance exists.
# Args: project name
# Output: "true" if the instance exists, "false" otherwise.
function psql_instance_exists() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud sql instances describe \
                --quiet --project="$project" \
                "$name" 2>&1
       ); then
        echo "true"
    elif [[ $output == *\ instance\ does\ not\ exist.* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Deploy a PostgreSQL instance if it doesn't exist
# Args: project name viewer
function psql_instance_deploy() {
    declare -r project="$1"; shift
    declare -r name="$1"; shift
    declare -r viewer="$1"; shift
    declare exists

    exists=$(psql_instance_exists "$project" "$name")
    if ! "$exists"; then
        # Get and cache the password in the current shell first
        password_get psql_superuser >/dev/null
        # Create the instance with the cached password
        # Where are your security best practices, Google?
        mute gcloud sql instances create \
            "$name" \
            --quiet \
            --project="$project" \
            --region="$PSQL_INSTANCE_REGION" \
            --tier="$PSQL_INSTANCE_TIER" \
            --assign-ip \
            --no-storage-auto-increase \
            --database-flags=cloudsql.iam_authentication=on \
            --root-password="$(password_get psql_superuser)" \
            --database-version=POSTGRES_14
    fi

    # Deploy the shared viewer user
    exists=$(psql_user_exists "$project" "$name" "$viewer")
    if ! "$exists" || password_is_specified psql_viewer; then
        # Get and cache the password in the current shell first
        password_get psql_viewer >/dev/null
        # Create the user with the cached password
        password_get psql_viewer |
            psql_user_deploy "$project" "$name" "$viewer"
    fi
}

# Execute a command with the PostgreSQL proxy providing the connection to a
# server with the "postgres" user. Setup environment variables for
# connection with only a database specification required, using libpq.
# Args: project instance command arg...
function psql_proxy_session() {
    # Source:
    # https://cloud.google.com/sql/docs/postgres/connect-admin-proxy#install
    declare -r url_base="https://dl.google.com/cloudsql/cloud_sql_proxy."
    declare -r -A url_os_sfx=(
        ["x86_64 GNU/Linux"]="linux.amd64"
        ["i386 GNU/Linux"]="linux.386"
    )
    declare -r project="$1"; shift
    declare -r instance="$1"; shift
    declare -r fq_instance="$project:$PSQL_INSTANCE_REGION:$instance"
    # The default proxy binary, if installed
    declare proxy="cloud_sql_proxy"
    declare pid
    declare pgpass

    # If we don't have the proxy in our path
    if ! command -v "$proxy" >/dev/null; then
        # If we don't have the proxy binary downloaded yet
        if ! [ -e "$PSQL_PROXY_BINARY" ]; then
            declare -r url="${url_base}${url_os_sfx[$(uname -m -o)]}"
            # Download the proxy binary
            mute wget --quiet -O "$PSQL_PROXY_BINARY" "$url"
            chmod 0755 "$PSQL_PROXY_BINARY"
        fi
        # Use the downloaded proxy
        proxy="$PSQL_PROXY_BINARY"
    fi

    # If we don't have the socket directory created yet
    if ! [ -e "$PSQL_PROXY_DIR" ]; then
        # Create the temporary directory
        mkdir "$PSQL_PROXY_DIR"
    fi

    # Start the proxy in background
    mute "$proxy" "-instances=$fq_instance" "-dir=$PSQL_PROXY_DIR" &
    pid="$!"
    # Store the PID of the shell running the proxy, for errexit cleanup
    echo -n "$pid" > "$PSQL_PROXY_SHELL_PID_FILE"

    # Create the .pgpass file
    touch "$PSQL_PROXY_PGPASS"
    chmod 0600 "$PSQL_PROXY_PGPASS"
    password_get_pgpass psql_superuser postgres >| "$PSQL_PROXY_PGPASS"

    # Wait for the proxy to become ready
    declare max_checks=10
    declare delay=3
    declare checks
    declare output=""
    declare ready="false"
    for ((checks=0; checks < max_checks; checks++)); do
        if output+=$(
            PGHOST="$PSQL_PROXY_DIR/$fq_instance" \
            PGPASSFILE="$PSQL_PROXY_PGPASS" \
            PGUSER="postgres" \
            pg_isready
        )$'\n'; then
            ready="true"
            break;
        fi
        sleep "$delay"
    done

    # Check if the wait was successful
    if ! "$ready"; then
        echo "PostgreSQL proxy ${proxy@Q} is not ready " \
             "after $((checks * delay)) seconds." >&2
        if [ -n "$output" ]; then
            echo "pg_isready output:" >&2
            echo "$output" >&2
        fi
    fi
    "$ready"

    # Run the command
    PGHOST="$PSQL_PROXY_DIR/$fq_instance" \
    PGPASSFILE="$PSQL_PROXY_PGPASS" \
    PGUSER="postgres" \
        "$@"

    # Remove the .pgpass file
    rm "$PSQL_PROXY_PGPASS"

    # Stop the proxy
    pkill -P "$pid" && wait "$pid" || true
    rm "$PSQL_PROXY_SHELL_PID_FILE"
}

# Check if a PostgreSQL database exists.
# Args: project instance database
# Output: "true" if the database exists, "false" otherwise.
function psql_database_exists() {
    declare -r project="$1"; shift
    declare -r instance="$1"; shift
    declare -r database="$1"; shift
    declare output
    if output=$(
            gcloud sql databases describe \
                --quiet --project="$project" \
                --instance="$instance" \
                "$database" 2>&1
       ); then
        echo "true"
    elif [[ $output == *\ Not\ Found* ]]; then
        echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Setup a PostgreSQL database and its paraphernalia.
# Expect the environment to be set up with variables permitting a libpq user
# to connect to the database as a superuser.
# Args: database init editor viewer
function _psql_database_setup() {
    declare -r database="$1"; shift
    declare -r init="$1"; shift
    declare -r editor="$1"; shift
    declare -r viewer="$1"; shift
    # Deploy viewer and editor permissions
    mute psql --dbname="$database" -e <<<"
        \\set ON_ERROR_STOP on

        GRANT USAGE ON SCHEMA public TO $editor, $viewer;

        ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $editor;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES
        IN SCHEMA public TO $editor;

        ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO $viewer;
        GRANT SELECT ON ALL TABLES
        IN SCHEMA public TO $viewer;
    "
    # Initialize the database, if requested
    if "$init"; then
        mute kcidb-db-init -lDEBUG -d "postgresql:dbname=$database" \
                           --ignore-initialized
    fi
}

# Cleanup a PostgreSQL database and its paraphernalia.
# Expect the environment to be set up with variables permitting a libpq user
# to connect to the database as a superuser.
# Args: database editor viewer
function _psql_database_cleanup() {
    declare -r database="$1"; shift
    declare -r editor="$1"; shift
    declare -r viewer="$1"; shift
    # Withdraw viewer and editor permissions
    mute psql --dbname="$database" -e <<<"
        /* Do not stop on errors in case users are already removed */
        REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public
        FROM $editor;
        REVOKE SELECT ON ALL TABLES IN SCHEMA public
        FROM $viewer;
        \\set ON_ERROR_STOP on
        /* Terminate all connections to the database except this one */
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = :'DBNAME' AND pid <> pg_backend_pid();
    "
    mute kcidb-db-cleanup -lDEBUG -d "postgresql:dbname=$database" \
                          --ignore-not-initialized \
                          --ignore-not-found
}

# Deploy PostgreSQL databases, if they don't exist
# Do not initialize databases that are prepended with the hash sign ('#').
# Args: project instance viewer [database editor]...
function psql_databases_deploy() {
    declare -r project="$1"; shift
    declare -r instance="$1"; shift
    declare -r viewer="$1"; shift
    declare database
    declare editor
    declare init
    declare exists

    while (($#)); do
        database="$1"; shift
        editor="$1"; shift

        # Handle possible leading hash sign
        if [[ $database == \#* ]]; then
            database="${database:1}"
            init=false
        else
            init=true
        fi

        # Create the database, if not exists
        exists=$(psql_database_exists "$project" "$instance" "$database")
        if ! "$exists"; then
            mute gcloud sql databases create \
                "$database" \
                --quiet \
                --project="$project" \
                --instance="$instance"
        fi

        # Deploy the per-database editor user
        exists=$(psql_user_exists "$project" "$instance" "$editor")
        if ! "$exists" || password_is_specified psql_editor; then
            # Get and cache the password in the current shell first
            password_get psql_editor >/dev/null
            # Create the user with the cached password
            password_get psql_editor |
                psql_user_deploy "$project" "$instance" "$editor"
        fi

        # NOTE: The viewer user is created per-instance

        # Setup the database
        psql_proxy_session "$project" "$instance" \
            _psql_database_setup "$database" "$init" "$editor" "$viewer"
    done
}

# Withdraw PostgreSQL databases, if they exist
# Cleanup all databases, even those prepended with the hash sign ('#').
# Args: project instance viewer [database editor]...
function psql_databases_withdraw() {
    declare -r project="$1"; shift
    declare -r instance="$1"; shift
    declare -r viewer="$1"; shift
    declare -a -r databases_and_editors=("$@")
    declare database
    declare editor

    # Cleanup and remove the databases
    set -- "${databases_and_editors[@]}"
    while (($#)); do
        # Ignore possible leading hash sign
        database="${1###}"; shift
        editor="$1"; shift
        exists=$(psql_database_exists "$project" "$instance" "$database")
        if "$exists"; then
            # Cleanup the database
            psql_proxy_session "$project" "$instance" \
                _psql_database_cleanup "$database" "$editor" "$viewer"
            # Delete the database
            mute gcloud sql databases delete \
                "$database" \
                --quiet \
                --project="$project" \
                --instance="$instance"
        fi
    done

    # Remove the users afterwards as they could be shared by databases
    set -- "${databases_and_editors[@]}"
    while (($#)); do
        # Discard the database name
        shift
        editor="$1"; shift
        # Withdraw the editor user
        psql_user_withdraw "$project" "$instance" "$editor"
        # NOTE: The viewer user is per-instance
    done
}

# Check if a PostgreSQL user exists
# Args: project instance name
function psql_user_exists() {
    declare -r project="$1"; shift
    declare -r instance="$1"; shift
    declare -r name="$1"; shift
    declare output
    if output=$(
            gcloud sql users list \
                --quiet --project="$project" \
                --instance="$instance" \
                --filter "name=$name" 2>&1
       ); then
        # Skip header / "Listed 0 items." message
        output=$(sed -e 1d <<<"$output")
        [ -n "$output" ] && echo "true" || echo "false"
    else
        echo "$output" >&2
        false
    fi
}

# Deploy a PostgreSQL user
# Args: project instance name
# Input: password
function psql_user_deploy() {
    declare -r project="$1"; shift
    declare -r instance="$1"; shift
    declare -r name="$1"; shift
    exists=$(psql_user_exists "$project" "$instance" "$name")
    if "$exists"; then
        mute gcloud sql users set-password \
            --quiet --project="$project" \
            --instance="$instance" \
            --prompt-for-password \
            "$name"
    else
        # Where are your security best practices, Google?
        mute gcloud sql users create \
            --quiet --project="$project" \
            --instance="$instance" \
            --password="$(cat)" \
            "$name"
    fi
    # Strip extra permissions added by gcloud by default
    psql_proxy_session "$project" "$instance" \
        mute psql --dbname postgres -e <<<"
            \\set ON_ERROR_STOP on
            REVOKE cloudsqlsuperuser FROM $name;
            ALTER ROLE $name WITH NOCREATEROLE NOCREATEDB;
        "
}

# Withdraw a PostgreSQL user
# Args: project instance name
function psql_user_withdraw() {
    declare -r project="$1"; shift
    declare -r instance="$1"; shift
    declare -r name="$1"; shift
    exists=$(psql_user_exists "$project" "$instance" "$name")
    if "$exists"; then
        mute gcloud sql users delete \
            --quiet --project="$project" \
            --instance="$instance" \
            "$name"
    fi
}

# Deploy (to) PostgreSQL.
# Do not initialize databases that are prepended with the hash sign ('#').
# Args: project [database editor]...
function psql_deploy() {
    declare -r project="$1"; shift
    # Deploy the instance
    psql_instance_deploy "$project" "$PSQL_INSTANCE" "$PSQL_VIEWER"
    # Deploy the databases
    psql_databases_deploy "$project" "$PSQL_INSTANCE" "$PSQL_VIEWER" "$@"
}

# Withdraw (from) PostgreSQL
# Cleanup all databases, even those prepended with the hash sign ('#').
# Args: project [database editor]...
function psql_withdraw() {
    declare -r project="$1"; shift
    psql_databases_withdraw "$project" "$PSQL_INSTANCE" "$PSQL_VIEWER" "$@"
    # NOTE: Leaving the instance behind. Its name has 7-day recycling period
    # NOTE: Leaving the viewer user behind, as it's per-instance
}

fi # _PSQL_SH
