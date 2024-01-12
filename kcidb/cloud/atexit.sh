# KCIDB cloud deployment - EXIT trap handling
#
if [ -z "${_ATEXIT_SH+set}" ]; then
declare _ATEXIT_SH=

# The list of commands to be executed on exit
declare -a ATEXIT=()

# Execute "atexit" commands, in reverse order.
function atexit_exec()
{
    declare i
    for ((i = ${#ATEXIT[@]} - 1; i >= 0; i--)); do
        eval "${ATEXIT[$i]}"
    done
}

# Execute atexit_exec on exit
trap atexit_exec EXIT

# Push a command to the stack of "atexit" commands.
# Args: command
function atexit_push()
{
    ATEXIT+=("$1")
}

# Pop the last command pushed to the stack of "atexit" commands.
function atexit_pop()
{
    unset ATEXIT[-1]
}

fi # _ATEXIT_SH
