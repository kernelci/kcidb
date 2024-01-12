# KCIDB cloud deployment - miscellaneous definitions
#
if [ -z "${_MISC_SH+set}" ]; then
declare _MISC_SH=

. aterr.sh
. atexit.sh

# The original TMPDIR value
declare -r TMPDIR_ORIG=${TMPDIR:-}

# The directory where all temporary files should be created
export TMPDIR=$(mktemp -d -t "kcidb_cloud.XXXXXXXXXX")
# Remove the directory with all the temporary files on exit
atexit_push "rm -Rf ${TMPDIR@Q}"

# Generate code declaring parameter variables with names and values passed
# through long-option command-line argument list, and assigning the positional
# arguments.
#
# Args: [param_name...] [-- [param_arg...]]
#
# Each parameter name ("param_name") must be a string matching the
# ^[A-Za-z_][A-Za-z0-9_]*?$ regex, specifying both the name of the option and
# the name of the local variable. However, all underscores ("_") are replaced
# with dashes ("-") in option names.
#
# Each parameter argument ("param_arg") is a command-line argument specifying
# long options and/or their values.
#
# Output: Code declaring parameter variables and assigning values to them.
#
function getopt_vars() {
    declare -a longopts=()
    declare -a params=()
    declare -A params_value=()
    declare arg
    declare param
    declare parsed_args

    # Parse parameter specifications
    while (($#)); do
        arg="$1"; shift
        if [ "$arg" == "--" ]; then
            break
        fi
        if ! [[ $arg =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            echo "Invalid parameter specification: ${arg@Q}" >&2
            return 1
        fi
        longopts+=("${arg//_/-}:")
        params+=("$arg")
    done

    # Parse parameter arguments
    parsed_args="$(IFS=","
                   getopt --name getopt_vars --options "" \
                          --longoptions "${longopts[*]}" \
                          -- "$@")"
    eval set -- "$parsed_args"
    while true; do
        case "$1" in
            --?*)
                param="${1:2}"
                param="${param//-/_}"
                params_value[$param]="$2"
                shift 2
                ;;
            --)
                shift
                break
                ;;
            *)
                echo "Unknown argument: ${1@Q}" >&2
                return 1
                ;;
        esac
    done

    # Generate code assigning parameters and checking for missing ones
    for param in "${params[@]}"; do
        if [[ -v params_value[$param] ]]; then
            echo "declare $param=${params_value[$param]@Q}"
        else
            echo "Required parameter missing: ${param@Q}" >&2
            return 1
        fi
    done

    # Generate code reassigning positional arguments
    echo "set -- ${@@Q}"
}

# Execute a command, capturing both stdout and stderr, and only outputting
# them both to stderr, if the command fails.
# Args: [argv...]
function mute() {
    declare output_file
    output_file=$(mktemp -t "XXXXXXXXXX.output")
    aterr_push """
        cat ${output_file@Q} >&2 || true
        rm -f ${output_file@Q}
    """
    "$@" >|"$output_file" 2>&1
    rm -f "$output_file"
    aterr_pop
}

# Escape backslashes and whitespace with backslashes in text.
#
# Input: The text to escape
# Output: The escaped text
function escape_whitespace() {
    sed -e 's/\\\|\s/\\&/g'
}

# "true" if verbose output is enabled, "false" otherwise
declare VERBOSE="false"

# Run a command with stdout redirected to stderr, if verbose output is
# enabled.
# Args: command [arg...]
function verbose() {
    if "$VERBOSE"; then
        "$@" >&2
    fi
}

# Delete a project's IAM policy binding, if it exists
# Args: project member role
function iam_policy_binding_withdraw() {
    declare -r project="$1"; shift
    declare -r member="$1"; shift
    declare -r role="$1"; shift
    declare output
    if ! output=$(
            gcloud projects remove-iam-policy-binding \
                --quiet "$project" --member="$member" --role="$role" 2>&1
       ) && [[ $output != *\ not\ found!* ]]; then
        echo "$output" >&2
        false
    fi
}

fi # _MISC_SH
