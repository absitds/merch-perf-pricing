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
. ${MODULE_HOME}/etc/bin/setup.shell.properties
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/hadoop-commands.sh


###############################################################################
#                                KMM                                          #
###############################################################################

test -f "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}"

exit_code=$?

if [ $exit_code == ${EXIT_CODE_FAIL} ]

then

  fn_create_local_directory "${BATCH_ID_DATA_DIR}"
  fn_get_batch_id
  echo "${BATCH_ID}" > "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}"

else

  export BATCH_ID=$(cat "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}")

fi

echo "Successfully Created Batch ID"



test -d  "${UNTAR_FILE_LOCATION}"

exit_code=$?

if [ $exit_code == ${EXIT_CODE_FAIL} ]

then

  fn_create_local_directory  "${UNTAR_FILE_LOCATION}"

else

   echo "Pexec extr temp directory already exists"
   fn_delete_recursive_local_directory "${UNTAR_FILE_LOCATION}"
   fn_create_local_directory  "${UNTAR_FILE_LOCATION}"

fi


echo "Successfully Created Pexec extr temp directory"



###############################################################################
#                                     End                                     #
###############################################################################
