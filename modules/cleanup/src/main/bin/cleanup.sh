
#!/bin/bash
###############################################################################
#                               Documentation                                 #
###############################################################################
#                                                                             #
# Description                                                                 #
#     :                                                                       #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################
#                           Identify Script Home                              #
###############################################################################

#Find the script file home
pushd . > /dev/null
SCRIPT_DIRECTORY="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_DIRECTORY}" ]);
do
  cd "`dirname "${SCRIPT_DIRECTORY}"`"
  SCRIPT_DIRECTORY="$(readlink "`basename "${SCRIPT_DIRECTORY}"`")";
done
cd "`dirname "${SCRIPT_DIRECTORY}"`" > /dev/null
SCRIPT_DIRECTORY="`pwd`";
popd  > /dev/null
MODULE_HOME="`dirname "${SCRIPT_DIRECTORY}"`"

###############################################################################
#                           Import Dependencies                               #
###############################################################################

if [ "${CONFIG_HOME}" == "" ]
then

     PROJECT_HOME="`dirname "${MODULE_HOME}"`"
     CONFIG_HOME="${PROJECT_HOME}/config"

fi

. ${CONFIG_HOME}/bash-env.properties
. ${MODULE_HOME}/bin/constants.sh
. ${MODULE_HOME}/bin/log-functions.sh
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/hadoop-functions.sh
. ${MODULE_HOME}/etc/config.sh

###############################################################################
#                       Common Environment Variables                          #
###############################################################################


test -f "${BATCH_ID_DATA_DIR}/${BATCH_ID_FILE_NAME}"

    exit_code=$?

    if [ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ];
    then
        fn_delete_local_file "${BATCH_ID_DATA_DIR}/${BATCH_ID_FILE_NAME}"
    fi

success_message="Successfully read batch_id"

failure_message="Failed to read batch_id"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}"


test -d "${UNTAR_FILE_LOCATION}"

    exit_code=$?

    if [ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ];
    then
        fn_delete_recursive_local_directory "${UNTAR_FILE_LOCATION}"
    fi

success_message="Successfully read pexec extr temp location"

failure_message="Failed to read pexec extr temp location"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_FALSE}"


###############################################################################
#                                     End                                     #
###############################################################################

