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
. ${MODULE_HOME}/bin/import-dependecies.sh
. ${MODULE_HOME}/bin/itds-price-tls-functions.sh
. ${MODULE_HOME}/etc/bin/pexec-cptolcl.shell.properties
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/hadoop-commands.sh
. ${MODULE_HOME}/bin/constants.sh

###############################################################################
#                                KMM                                          #
###############################################################################
fn_get_persisted_batch_id

fn_delete_recursive_local_directory ${LOCAL_PEXEC_OUT_DIR}/${BATCH_ID} ${BOOLEAN_FALSE}

fn_create_local_directory ${LOCAL_PEXEC_OUT_DIR}/${BATCH_ID} ${BOOLEAN_TRUE}

fn_hadoop_download_file ${DFS_PEXEC_OUT_DIR}/${BATCH_ID}/* ${LOCAL_PEXEC_OUT_DIR}/${BATCH_ID} ${BOOLEAN_TRUE}


###############################################################################
#                                     End                                     #
###############################################################################
