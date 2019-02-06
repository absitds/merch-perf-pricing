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
. ${MODULE_HOME}/etc/bin/pexec-cptoshpt.shell.properties
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/hadoop-commands.sh
. ${MODULE_HOME}/bin/constants.sh


###############################################################################
#                                KMM                                          #
###############################################################################

fn_get_persisted_batch_id

YEAR="${BATCH_ID:0:4}"

MONTH="${BATCH_ID:4:2}"

DAY="${BATCH_ID:6:2}"

EXPECTED_DATE="${YEAR}-${MONTH}-${DAY}"

fn_upload_all_dir_to_shpt "${EXPECTED_DATE}" "${LOCAL_PEXEC_OUT_DIR}/${BATCH_ID}" "sranj04@safeway.com" "Exadatum@2019" "${MODULE_HOME}/lib/pexec-cptoshpt.jar" "com.albertsons.itds.pexec.sharepoint.SharePointUploader"

###############################################################################
#                                     End                                     #
###############################################################################
