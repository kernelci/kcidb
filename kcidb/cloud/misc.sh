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

# Evaluate and execute a command string,
# exit shell with error message and status 1 if unsuccessfull.
# Args: [eval_arg]...
function assert()
{
    # Use private global-style variable names
    # to avoid clashes with "evaled" names
    declare _ASSERT_ATTRS
    declare _ASSERT_STATUS

    # Prevent shell from exiting due to `set -e` if the command fails
    read -rd '' _ASSERT_ATTRS < <(set +o) || [ $? == 1 ]
    set +o errexit
    (
        eval "$_ASSERT_ATTRS"
        eval "$@"
    )
    _ASSERT_STATUS=$?
    eval "$_ASSERT_ATTRS"

    if [ "$_ASSERT_STATUS" != 0 ]; then
        echo "Assertion failed: $*" >&1
        exit 1
    fi
}

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

# Quote a string according to YAML double-quoted style
# Args: string_to_quote
function yaml_quote() {
    declare -r str="$1"; shift
    declare python_code='import sys, yaml; yaml.dump('
    python_code+='sys.argv[1], default_style="\"", stream=sys.stdout'
    python_code+=')'
    python3 -c "$python_code" "$str"
}

# Validate JSON against a schema
# Input: The JSON to validate
# Args: schema
function json_validate() {
    declare -r schema="$1"; shift
    declare python_code='import sys, json, jsonschema; '
    python_code+='schema_file = open(sys.argv[1], "r"); '
    python_code+='schema = json.load(schema_file); '
    python_code+='jsonschema.validate('
    python_code+='instance=json.load(sys.stdin), '
    python_code+='schema=schema, '
    python_code+='format_checker=jsonschema.Draft7Validator.FORMAT_CHECKER'
    python_code+=')'
    python3 -c "$python_code" "$schema"
}

fi # _MISC_SH
