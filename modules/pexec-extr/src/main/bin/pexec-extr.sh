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

. ${MODULE_HOME}/bin/import-dependecies.sh
. ${MODULE_HOME}/bin/itds-price-tls-functions.sh
. ${MODULE_HOME}/etc/bin/pexec-extr.shell.properties

###############################################################################
#                                KMM                                          #
###############################################################################
fn_get_persisted_batch_id

REPORT_ID="${BATCH_ID}"



test -d  "${PEXEC_PROD_TEMP}"

exit_code=$?

if [ $exit_code == ${EXIT_CODE_FAIL} ]

then

  fn_create_local_directory  "${PEXEC_PROD_TEMP}"

else

   fn_delete_recursive_local_directory "${PEXEC_PROD_TEMP}"
   fn_create_local_directory  "${PEXEC_PROD_TEMP}"

fi


test -d  "${PEXEC_EXTTR_OUT_TEMP_PATH}"

exit_code=$?

if [ $exit_code == ${EXIT_CODE_FAIL} ]

then

  fn_create_local_directory  "${PEXEC_EXTTR_OUT_TEMP_PATH}"

fi

#execute extraction script

sh "${PEXEC_SCRIPT_PATH}/${SCRIPT_NAME}" -l "${REPORT_ID}"

exit_code=$?
success_message="Extraction completed successfully"
failure_message="Extraction Job has been failed"
fail_on_error=true

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

fn_untar_to_target_directory "${BATCH_ID}" "${PEXEC_EXTR_OUT_PATH}" "${UNTAR_FILE_LOCATION}"

fn_delete_recursive_local_directory "${LOCAL_PEXEC_IN_DIR}/${BATCH_ID}" ${BOOLEAN_FALSE}

fn_create_local_directory "${LOCAL_PEXEC_IN_DIR}/${BATCH_ID}" ${BOOLEAN_TRUE}

fn_unzip_gz_file_to_target_directory "${UNTAR_FILE_LOCATION}" "${LOCAL_PEXEC_IN_DIR}/${BATCH_ID}"

###############################################################################
#                                     End                                     #
###############################################################################
