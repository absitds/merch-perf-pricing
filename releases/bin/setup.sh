#!/bin/bash
###############################################################################
#                               Documentation                                 #
###############################################################################
#                                                                             #
# Description                                                                 #
#     : It executes component specific setup scripts.                         #
#       Setup scripts should be present in the scripts folder. For very       #
#       compoenent there shall be seperate shell script. Setup scripts may    #
#       create, update, delete component specific metadata.                   #
#       User can only run component specific scripts by passing the second    #
#       argument to this script. All component scripts can be executed by     #
#       passign option all to this script.                                    #
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

###############################################################################
#                       Common Environment Variables                          #
###############################################################################

export RELEASE_HOME_DIRECTORY="`dirname "${SCRIPT_DIRECTORY}"`"

export PROJECT_HOME_DIRECTORY="`dirname "${RELEASE_HOME_DIRECTORY}"`"

export MODULE_HOME="${RELEASE_HOME_DIRECTORY}"

if [ "${CONFIG_HOME}" == "" ]
then

  CONFIG_HOME="${PROJECT_HOME_DIRECTORY}/config"

fi

. ${CONFIG_HOME}/bash-env.properties

. ${MODULE_HOME}/bin/constants.sh
. ${MODULE_HOME}/bin/log-functions.sh
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/hadoop-functions.sh
. ${MODULE_HOME}/bin/itds-price-tls-functions.sh
###############################################################################
#                           Import Dependencies                               #
###############################################################################
#Load user defined environment variables
. ${RELEASE_HOME_DIRECTORY}/etc/setup-config.sh
###############################################################################
#                           Validate the setup                                #
###############################################################################
fn_assert_executable_exists "java" "${BOOLEAN_TRUE}"
###############################################################################
#                           Import Dependencies                               #
###############################################################################

function fn_setup_local(){

  fn_assert_variable_is_set "release" "$1"

  fn_log_info "Running Local setup script"

  . ${RELEASE_HOME_DIRECTORY}/versions/"$1"/scripts/setup-local.sh

  exit_code=$?

  fail_on_error=${BOOLEAN_TRUE}

  success_message="Successfully executed Local setup script"

  failure_message="Failed to execute Local setup script"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_setup_hdfs(){

  fn_assert_variable_is_set "release" "$1"

  fn_log_info "Running HDFS setup script"

  . ${RELEASE_HOME_DIRECTORY}/versions/"$1"/scripts/setup-hdfs.sh

  exit_code=$?

  fail_on_error=${BOOLEAN_TRUE}

  success_message="Successfully executed HDFS setup script"

  failure_message="Failed to execute HDFS setup script"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_setup_kafka(){

  fn_assert_variable_is_set "release" "$1"

  fn_log_info "Running KAFKA setup script"

  . ${RELEASE_HOME_DIRECTORY}/versions/"$1"/scripts/setup-kafka.sh

  exit_code=$?

  fail_on_error=${BOOLEAN_TRUE}

  success_message="Successfully executed KAFKA setup script"

  failure_message="Failed to execute KAFKA setup script"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_setup_hive(){

  fn_assert_variable_is_set "release" "$1"

  fn_log_info "Running HIVE setup script"

  . ${RELEASE_HOME_DIRECTORY}/versions/"$1"/scripts/setup-hive.sh

  exit_code=$?

  fail_on_error=${BOOLEAN_TRUE}

  success_message="Successfully executed HIVE setup script"

  failure_message="Failed to execute HIVE setup script"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_setup_all(){

  fn_assert_variable_is_set "release" "$1"

  fn_log_info "Running all setup scripts"

  fn_setup_local "$1"

  fn_setup_hdfs "$1"

  fn_setup_kafka "$1"

  fn_setup_hive "$1"

  fn_setup_redshift "$1"

  fn_log_info "Successfully executed all setup scripts"

}



# Entry point for this script.
#
function fn_main(){
  command="$1"
  case "${command}" in

    local)
      fn_setup_local "${@:2}"
    ;;

    hdfs)
      fn_setup_hdfs "${@:2}"
    ;;

    kafka)
      fn_setup_kafka "${@:2}"
    ;;

    hive)
      fn_setup_hive "${@:2}"
    ;;

    all)
      fn_setup_all "${@:2}"
    ;;

    *)
      echo $"Usage: $0 {local|hdfs|kafka|hive|all}"
      exit 1

  esac

}


fn_main "$@"
################################################################################
#                                     End                                      #
################################################################################