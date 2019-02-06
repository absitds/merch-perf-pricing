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
. ${MODULE_HOME}/etc/bin/pexec-xlsgen.shell.properties
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/hadoop-commands.sh
. ${MODULE_HOME}/bin/constants.sh

###############################################################################
#                                KMM                                          #
###############################################################################
fn_get_persisted_batch_id

DATA_PATH=${DFS_PEXEC_IN_DIR}/${BATCH_ID}

JOB_NAME=perf-exex-tool

NUM_EXCEL_WRITERS=80

fn_assert_file_exists ${MODULE_HOME}/lib/pexec-xlsgen.jar ${BOOLEAN_TRUE}

fn_run_mapreduce_job ${MODULE_HOME}/lib/pexec-xlsgen.jar com.albertsons.itds.loyalty.pexec.PerfExecTool "$DATA_PATH" "$DIVISION_FILE_PATH" "$JOB_NAME" "$NUM_EXCEL_WRITERS" "${DFS_PEXEC_OUT_DIR}/${BATCH_ID}" "$BATCH_ID" "$SCHEMA_FILE_PATH" "$TEMPLATE_FILE_PATH"

###############################################################################
#                                     End                                     #
###############################################################################
