# KCIDB cloud deployment - password management
#
if [ -z "${_PASSWORD_SH+set}" ]; then
declare _PASSWORD_SH=

. secret.sh
. misc.sh

# A map of password names and their descriptions
declare -r -A PASSWORD_DESCS=(
    [smtp]="SMTP"
    [psql_superuser]="PostgreSQL superuser"
    [psql_editor]="PostgreSQL editor user"
    [psql_viewer]="PostgreSQL viewer user"
)

# A map of password names and their "can be auto-generated" flags.
# The corresponding password will be auto-generated if the flag is "true", and
# no source file nor secret was specified for it.
declare -A PASSWORD_GENERATE=(
    [psql_editor]="true"
    [psql_viewer]="true"
)

# A map of password names and their project and secret names, separated by a
# colon. Used for retrieving passwords if they have no source files specified.
declare -A PASSWORD_SECRETS=()

# A map of password names and their source files
declare -A PASSWORD_FILES=()

# A map of password names and their strings
declare -A PASSWORD_STRINGS=()

# A map of password names and their update status
declare -A PASSWORD_UPDATED=()

# Check that every specified password exists.
# Args: name...
function password_exists() {
    declare name
    while (($#)); do
        name="$1"; shift
        if ! [[ -v PASSWORD_DESCS[$name] ]]; then
            return 1
        fi
    done
    return 0
}

# Ask the user to input a password with specified name.
# Args: name
# Output: The retrieved password
function password_input() {
    declare -r name="$1"; shift
    assert password_exists "$name"
    declare password
    read -p "Enter ${PASSWORD_DESCS[$name]:-a} password: " -r -s password
    echo "" >&2
    echo -n "$password"
}

# Get a password with specified name, either from the cache, from its source
# file, from its secret, or from the user. Make sure the retrieved password is
# cached.
# Args: name
# Output: The retrieved password
function password_get() {
    declare -r name="$1"; shift
    assert password_exists "$name"

    declare password
    declare -r password_file="${PASSWORD_FILES[$name]:-}"
    declare -r password_secret="${PASSWORD_SECRETS[$name]:-}"
    declare password_secret_exists
    password_secret_exists=$(
        if [ -n "$password_secret" ]; then
            secret_exists "${password_secret%%:*}" "${password_secret#*:}"
        else
            echo "false"
        fi
    )
    declare -r password_secret_exists
    declare -r password_generate="${PASSWORD_GENERATE[$name]:-false}"

    # If cached
    if [[ -v PASSWORD_STRINGS[$name] ]]; then
        password="${PASSWORD_STRINGS[$name]}"
    # If file is specified
    elif [ -n "$password_file" ]; then
        # If asked to read from standard input
        if [ "$password_file" == "-" ]; then
            password=$(password_input "$name")
        else
            password=$(<"$password_file")
        fi
        PASSWORD_UPDATED[$name]="true"
    # If secret exists
    elif "$password_secret_exists"; then
        password=$(
            secret_get "${password_secret%%:*}" "${password_secret#*:}"
        )
    # If can be generated
    elif "$password_generate"; then
        password=$(dd if=/dev/random bs=32 count=1 status=none | base64)
        PASSWORD_UPDATED[$name]="true"
    # Else read from user
    else
        password=$(password_input "$name")
        PASSWORD_UPDATED[$name]="true"
    fi

    PASSWORD_STRINGS[$name]="$password"

    echo -n "$password"
}

# Get the passwords with the specified names as a PostgreSQL's .pgpass file,
# generated with the corresponding specified usernames.
# Args: [name username]...
# Output: The generated .pgpass file
function password_get_pgpass() {
    declare -r -a escape_argv=(sed -e 's/[:\\]/\\&/g')
    declare name
    declare username

    while (($#)); do
        name="$1"; shift
        assert password_exists "$name"
        username="$1"; shift

        # Cache the password in the current shell
        password_get "$name" > /dev/null

        # Output the pgpass line
        echo -n "*:*:*:"
        echo -n "$username" | "${escape_argv[@]}"
        echo -n ":"
        password_get "$name" | "${escape_argv[@]}"
    done
}

# Set the source file for a password with specified name. The file will be
# used as the source of the password by password_get, if it wasn't already
# retrieved (and cached) before. Can be specified as "-" to have password
# requested from standard input.
# Args: name file
function password_set_file() {
    declare -r name="$1"; shift
    assert password_exists "$name"
    declare -r file="$1"; shift
    PASSWORD_FILES[$name]="$file"
}

# Set the project and the name of the secret storing the password with
# specified name. The password will be retrieved from the secret, if it wasn't
# cached, and if its source file wasn't specified.
# Args: name project secret
function password_secret_set() {
    declare -r name="$1"; shift
    declare -r project="$1"; shift
    declare -r secret="$1"; shift
    assert password_exists "$name"
    if [[ "$project" = *:* ]]; then
        echo "Invalid project name ${project@Q}" >&2
        exit 1
    fi
    PASSWORD_SECRETS[$name]="$project:$secret"
}

# Check if every specified password has its secret set
# Assumes every specified password is known/exists.
# Args: name...
function password_secret_is_specified() {
    declare name
    assert password_exists "$@"
    while (($#)); do
        name="$1"; shift
        if ! [[ -v PASSWORD_SECRETS[$name] ]]; then
            return 1
        fi
    done
    return 0
}

# Check if every specified password's secret exists.
# Assumes every specified password is known/exists and has its secret set.
# Args: name...
# Output: "true" if all secrets exists, "false" otherwise.
function password_secret_exists() {
    declare name
    declare project
    declare secret
    declare exists
    assert password_exists "$@"
    assert password_secret_is_specified "$@"
    while (($#)); do
        name="$1"; shift
        project="${PASSWORD_SECRETS[$name]%%:*}"
        secret="${PASSWORD_SECRETS[$name]#*:}"
        exists=$(secret_exists "$project" "$secret")
        if ! "$exists"; then
            echo false
            return
        fi
    done
    echo true
}

# Specify the single-word command returning exit status specifying if the
# password with specified name could be auto-generated or not.
# Args: name generate
function password_set_generate() {
    declare -r name="$1"; shift
    declare -r generate="$1"; shift
    assert password_exists "$name"
    PASSWORD_GENERATE[$name]="$generate"
}

# Check if any of the passwords with specified names are explicitly specified
# by the command-line user. That is, if any of them has a source file.
# Args: name...
function password_is_specified() {
    declare name
    assert password_exists "$@"
    while (($#)); do
        name="$1"; shift
        if ! [[ -v PASSWORD_FILES[$name] ]]; then
            return 1
        fi
    done
    return 0
}

# Check if any of the passwords with specified names are (to be) updated.
# Args: name...
# Output: "true" if all secrets exists, "false" otherwise.
function password_is_updated() {
    declare name
    declare secret_exists
    assert password_exists "$@"
    while (($#)); do
        name="$1"; shift
        # If the password was updated
        if "${PASSWORD_UPDATED[$name]:-false}"; then
            echo true
            return
        fi
        # If the password is going to be read from a file
        if password_is_specified "$name"; then
            echo true
            return
        fi
        if password_secret_is_specified "$name"; then
            secret_exists="$(password_secret_exists "$name")"
            if "$secret_exists"; then
                # We're going to use the password's secret
                continue
            fi
        fi
        echo true
        return
    done
    echo false
}

# Deploy passwords to their secrets (assuming they're set with
# "password_secret_set"). For every password deploy only if the password is
# updated, or the secret doesn't exist.
# Args: name...
function password_secret_deploy() {
    declare name
    declare updated
    declare project
    declare secret
    assert password_exists "$@"
    assert password_secret_is_specified "$@"
    while (($#)); do
        name="$1"; shift
        updated=$(password_is_updated "$name")
        if "$updated"; then
            # Get and cache the password in the current shell first
            password_get "$name" > /dev/null
            # Deploy the cached password
            project="${PASSWORD_SECRETS[$name]%%:*}"
            secret="${PASSWORD_SECRETS[$name]#*:}"
            password_get "$name" | secret_deploy "$project" "$secret"
        fi
    done
}

# Withdraw passwords from their secrets (assuming they're set with
# "password_secret_set").
# Args: name...
function password_secret_withdraw() {
    declare name
    declare project
    declare secret
    assert password_exists "$@"
    while (($#)); do
        name="$1"; shift
        if ! [[ -v PASSWORD_SECRETS[$name] ]]; then
            echo "Password ${name@Q} has no secret specified" >&2
            exit 1
        fi
        project="${PASSWORD_SECRETS[$name]%%:*}"
        secret="${PASSWORD_SECRETS[$name]#*:}"
        secret_withdraw "$project" "$secret"
    done
}

# Deploy passwords (with corresponding user names) as a pgpass secret.
# Deploy only if one of the passwords is specified, or if the pgpass secret
# doesn't exist.
# Args: project pgpass_secret [password_name user_name]...
function password_secret_deploy_pgpass() {
    declare -r project="$1"; shift
    declare -r pgpass_secret="$1"; shift
    declare -a -r password_and_user_names=("$@")
    declare -a password_names
    while (($#)); do
        password_names+=("$1")
        shift 2
    done
    declare new_pgpass
    declare exists

    # Generate the (potentially) new pgpass with cached passwords
    new_pgpass="$(password_get_pgpass "${password_and_user_names[@]}")"

    # If the secret already exists
    exists=$(secret_exists "$project" "$pgpass_secret")
    if "$exists"; then
        declare old_pgpass
        # Retrieve the current pgpass
        old_pgpass="$(secret_get "$project" "$pgpass_secret")"
        # If the pgpass hasn't changed
        if [ "$new_pgpass" == "$old_pgpass" ]; then
            # Don't deploy
            return
        fi
    fi

    # Deploy the .pgpass
    secret_deploy "$project" "$pgpass_secret" <<<"$new_pgpass"
}

fi # _PASSWORD_SH
