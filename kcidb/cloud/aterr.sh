# KCIDB cloud deployment - ERR trap handling
#
if [ -z "${_ATERR_SH+set}" ]; then
declare _ATERR_SH=

# Make subshells inherit ERR traps
set -E

# The list of pairs of shell PIDs and commands to be executed on error exits
# from the corresponding shells, separated by a colon.
declare -a ATERR=()

# Execute "aterr" commands belonging to the current shell, in reverse order.
function aterr_exec()
{
    declare i
    for ((i = ${#ATERR[@]} - 1; i >= 0; i--)); do
        if [ "${ATERR[i]%%:*}" == "$BASHPID" ]; then
            eval "${ATERR[i]#*:}"
        fi
    done
}

# Execute aterr_exec on error exit enabled with "set -e".
# Expect "set -E" propagating the ERR trap everywhere.
trap aterr_exec ERR

# Push a command to the stack of "aterr" commands. The command will be
# executed on error exit from the current shell only.
# Args: command
function aterr_push()
{
    declare -r command="$1"; shift
    ATERR+=("$BASHPID:$command")
}

# Pop the last command pushed to the stack of "aterr" commands.
# Only pops the command if it was pushed by the same shell.
function aterr_pop()
{
    if [ "${ATERR[-1]%%:*}" == "$BASHPID" ]; then
        unset ATERR[-1]
    fi
}

fi # _ATERR_SH
