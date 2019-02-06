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
. ${MODULE_HOME}/etc/bin/pexec-cptohd.shell.properties
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/hadoop-commands.sh
. ${MODULE_HOME}/bin/constants.sh

###############################################################################
#                                KMM                                          #
###############################################################################

fn_get_persisted_batch_id

fn_delete_hdfs_directory ${DFS_PEXEC_IN_DIR}/${BATCH_ID} ${BOOLEAN_TRUE}

fn_create_hdfs_directory ${DFS_PEXEC_IN_DIR}/${BATCH_ID} ${BOOLEAN_TRUE}

fn_copy_all_file_from_local_to_hadoop "${LOCAL_PEXEC_IN_DIR}/${BATCH_ID}" ${DFS_PEXEC_IN_DIR}/${BATCH_ID} ${BOOLEAN_TRUE}


###############################################################################
#                                     End                                     #
###############################################################################
