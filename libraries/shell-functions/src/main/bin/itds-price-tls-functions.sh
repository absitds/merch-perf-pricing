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
#                                  Functions                                  #
###############################################################################

function fn_get_batch_id(){
    export BATCH_ID=$(date '+%Y%m%d')
}

function fn_get_persisted_batch_id(){

    test -f "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}"

     exit_code=$?
     if [ $exit_code == ${EXIT_CODE_FAIL} ]

     then

        fn_handle_exit_code "${exit_code}" "BatchId Exists" "BatchID not exists" "${fail_on_error}"

     else

      export BATCH_ID=$(cat "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}")

     fi

     export BATCH_ID=$(cat "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}")

}

function fn_run_gobblin(){

  pull_file=$1

  fail_on_error=$2

  fn_assert_variable_is_set "pull_file" "${pull_file}"

  fn_assert_variable_is_set "GOBBLIN_HOME" "${GOBBLIN_HOME}"

  bash ${GOBBLIN_HOME}/bin/gobblin-mapreduce.sh --conf  ${pull_file}

  exit_code=$?

  success_message="Successfully ran gobblin job "

  failure_message="Failed to run gobblin job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_run_custom_camus(){

  camus_properties=$1

  fail_on_error=$2

  fn_assert_variable_is_set "camus_properties" "${camus_properties}"

  fn_assert_variable_is_set "CAMUS_HOME" "${CAMUS_HOME}"

  bash ${CAMUS_HOME}/bin/camus-run -P  ${camus_properties}

  exit_code=$?

  success_message="Successfully ran camus job "

  failure_message="Failed to run camus job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}



function fn_run_spark(){

  spark_class=$1

  jar=$2

  configuration_file=$3

  batch_id=$4

  fail_on_error=$5

  fn_assert_executable_exists "spark-submit" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "jar" "${jar}"

  spark-submit \
       --class ${spark_class} \
       --master yarn-client \
       --conf spark.hadoop.validateOutputSpecs=false \
       ${jar} ${configuration_file} ${batch_id}

  exit_code=$?

  success_message="Successfully ran spark transform job "

  failure_message="Failed to run spark transform job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


function fn_run_spark_transform_on_yarn(){

  spark_class=$1

  jar=$2

  external_jar=$3

  external_file=$4

  input_path=$5

  output_path=$6

  xsd=$7

  fail_on_error=$8

  fn_assert_executable_exists "spark-submit" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "jar" "${jar}"

  fn_assert_variable_is_set "external_jar" "${external_jar}"

  fn_assert_variable_is_set "external_file" "${external_file}"

  fn_assert_variable_is_set "input_path" "${input_path}"

  fn_assert_variable_is_set "output_path" "${output_path}"

  fn_assert_variable_is_set "xsd" "${xsd}"

  spark-submit \
       --class ${spark_class} \
       --master yarn-client \
       --conf spark.hadoop.validateOutputSpecs=false \
       --jars ${external_jar} \
       --files ${external_file} \
       ${jar} ${input_path} ${output_path} ${xsd} ${external_file}

  exit_code=$?

  success_message="Successfully ran spark transform job "

  failure_message="Failed to run spark transform job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_run_spark_transform_on_yarn_custom(){

  spark_class=$1

  jar=$2

  external_file=$3

  input_path=$4

  output_path=$5

  xsd=$6

  fail_on_error=$7

  fn_assert_executable_exists "spark-submit" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "jar" "${jar}"

  fn_assert_variable_is_set "external_file" "${external_file}"

  fn_assert_variable_is_set "input_path" "${input_path}"

  fn_assert_variable_is_set "output_path" "${output_path}"

  fn_assert_variable_is_set "xsd" "${xsd}"

  spark-submit \
       --class ${spark_class} \
       --master yarn-client \
       --conf spark.hadoop.validateOutputSpecs=false \
       --files ${external_file} \
       ${jar} ${input_path} ${output_path} ${xsd} ${external_file}

  exit_code=$?

  success_message="Successfully ran spark transform job "

  failure_message="Failed to run spark transform job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


function fn_run_java(){

  classpath=$1

  classname=$2

  variable=${@:3}

  fn_assert_executable_exists "java" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "classpath" "${classpath}"

  fn_assert_variable_is_set "classname" "${classname}"

  java -cp ${classpath} ${classname} ${variable}

  exit_code=$?

  success_message="Successfully ran ${classname} "

  failure_message="Failed to run ${classname}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

fn_run_mapreduce_job(){

JAR_FILE=$1
CLASS_NAME=$2
DATA_PATH=$3
DIVISION_FILE_PATH=$4
JOB_NAME=$5
NUM_EXCEL_WRITERS=$6
OUTPUT_DIR=$7
REPORT_DATE=$8
SCHEMA_FILE_PATH=$9
TEMPLATE_FILE_PATH=${10}

fn_assert_variable_is_set "JAR_FILE" "${JAR_FILE}"
fn_assert_variable_is_set "CLASS_NAME" "${CLASS_NAME}"
fn_assert_variable_is_set "DATA_PATH" "${DATA_PATH}"
fn_assert_variable_is_set "DIVISION_FILE_PATH" "${DIVISION_FILE_PATH}"
fn_assert_variable_is_set "JOB_NAME" "${JOB_NAME}"
fn_assert_variable_is_set "NUM_EXCEL_WRITERS" "${NUM_EXCEL_WRITERS}"
fn_assert_variable_is_set "OUTPUT_DIR" "${OUTPUT_DIR}"
fn_assert_variable_is_set "REPORT_DATE" "${REPORT_DATE}"
fn_assert_variable_is_set "SCHEMA_FILE_PATH" "${SCHEMA_FILE_PATH}"
fn_assert_variable_is_set "TEMPLATE_FILE_PATH" "${TEMPLATE_FILE_PATH}"


hadoop jar $JAR_FILE $CLASS_NAME -Dmapreduce.reduce.memory.mb=4120 -Dmapreduce.reduce.java.opts=-Xmx5560m  --data-path $DATA_PATH --divisions-file-path $DIVISION_FILE_PATH --job-name $JOB_NAME --num-excel-writers $NUM_EXCEL_WRITERS --output-path  $OUTPUT_DIR --report-date $REPORT_DATE --schema-file-path  $SCHEMA_FILE_PATH --template-file-path $TEMPLATE_FILE_PATH

exit_code=$?

  if [[ "${exit_code}" != "${EXIT_CODE_SUCCESS}" ]];then

        fn_delete_hdfs_directory "${OUTPUT_DIR}"
        fn_exit_with_failure_message "1" "unable to generate excel files"
  else
        echo "SUCCESS generated excel files to : ${OUTPUT_DIR}"
  fi

}

function fn_copy_local_file() {

    file_path="$1"

    target_dir="$2"

    fn_assert_variable_is_set "file_path" "$file_path"

    fn_assert_variable_is_set "target_dir" "$target_dir"

    fail_on_error=$3

    cp  $file_path $target_dir

    exit_code=$?

    if [ $exit_code == 0 ]; then

        success_message="Successfully copy the file $file_path to $target_dir"

    else

        failure_message="Failed to copy the $file_path to $target_dir"

    fi

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


function fn_untar_to_target_directory(){

 BATCH_ID="$1"

 fn_assert_variable_is_set "BATCH_ID" "${BATCH_ID}"

YEAR="${BATCH_ID:0:4}"

MONTH="${BATCH_ID:4:2}"

DAY="${BATCH_ID:6:2}"

EXPECTED_DATE="${YEAR}-${MONTH}-${DAY}"

src_pth="$2"

trgt_pth="$3"

fn_assert_variable_is_set "src_pth" "$src_pth"

fn_assert_variable_is_set "trgt_pth" "$trgt_pth"

fn_assert_variable_is_set "EXPECTED_DATE" "$EXPECTED_DATE"

file_name=$(ls "$src_pth" | grep -i "$EXPECTED_DATE" |grep -i ".tar" | rev | cut -d ' ' -f1 | rev)

exit_code=$?

if [[ ${exit_code} = ${EXIT_CODE_SUCCESS} ]]; then

success_message="Successfully get file name from the src path"

fi

export IFS=$'\n'
for file in ${file_name}; do

tar -xvf "${src_pth}/${file}" -C "${trgt_pth}"

done

exit_code=$?

success_message="Successfully untar the file from ${src_pth} to target ${trgt_pth}"

failure_message="Failed to untar the file from ${src_pth} to target  ${trgt_pth}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}

function fn_unzip_gz_file_to_target_directory(){

src_pth="$1"

trgt_pth="$2"

fn_assert_variable_is_set "src_pth" "$src_pth"

fn_assert_variable_is_set "trgt_pth" "$trgt_pth"

file_name=$(ls "$src_pth" |grep -i ".dat.gz" | rev | cut -d ' ' -f1 | rev)

exit_code=$?

if [[ ${exit_code} = 0 ]]; then

echo success_message="Successfully get file name from the src path"

fi
export IFS=$'\n'
for file in ${file_name}; do
new_file_name=$(echo $file | rev | cut -c 4- | rev)
gunzip -c "${src_pth}/${file}" > "${trgt_pth}"/"${new_file_name}"
done

exit_code=$?
success_message="Successfully unzip the gz files from ${src_pth} to target location ${trgt_pth}"

failure_message="Failed to unzip the gz files from ${src_pth} to ${trgt_pth}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}

fn_copy_all_file_from_local_to_hadoop(){

src_pth="$1"

trgt_pth="$2"

fail_on_error="$3"

fn_assert_variable_is_set "src_pth" "$src_pth"

fn_assert_variable_is_set "trgt_pth" "$trgt_pth"

fn_assert_variable_is_set "fail_on_error" "$fail_on_error"

file_name=$(ls "$src_pth")

export IFS=$'\n'

for file in ${file_name}; do

fn_copy_file_from_local_to_hadoop "${src_pth}/${file}" ${trgt_pth} ${fail_on_error}

done

}

fn_upload_dir_to_shpt(){

  BATCH_ID="$1"

  source_dir_path="$2"

  user_name="$3"

  passwd="$4"

  jar_path="$5"

  class_name="$6"

  fn_assert_variable_is_set "BATCH_ID" "$BATCH_ID"

  fn_assert_variable_is_set "source_dir_path" "$source_dir_path"

  fn_assert_variable_is_set "user_name" "$user_name"

  fn_assert_variable_is_set "passwd" "$passwd"

  fn_assert_variable_is_set "jar_path" "$jar_path"

  fn_assert_variable_is_set "class_name" "$class_name"

  java -cp "$jar_path" "$class_name" "$user_name" "$passwd" "${BATCH_ID}" "${source_dir_path}"

  exit_code=$?

  success_message="Successfully uploaded ${source_dir_path} "

  failure_message="Failed to upload ${source_dir_path} on sharePoint"

  fail_on_error=true

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_upload_all_dir_to_shpt(){

  BATCH_ID="$1"

  src_pth="$2"

  user_name="$3"

  passwd="$4"

  jar_path="$5"

  class_name="$6"

  fn_assert_variable_is_set "BATCH_ID" "$BATCH_ID"

  fn_assert_variable_is_set "src_pth" "$src_pth"

  fn_assert_variable_is_set "user_name" "$user_name"

  fn_assert_variable_is_set "passwd" "$passwd"

  fn_assert_variable_is_set "jar_path" "$jar_path"

  fn_assert_variable_is_set "class_name" "$class_name"

  sub_dir_list=$(ls "$src_pth")

  exit_code=$?

  success_message="Successfully got sub directories "

  failure_message="Failed to get sub directories"

  fail_on_error=true

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

export IFS=$'\n'
 for dir_name in $sub_dir_list; do
    source_dir_path="${src_pth}/${dir_name}"
    fn_upload_dir_to_shpt "${BATCH_ID}" "${source_dir_path}" "${user_name}" "${passwd}" "${jar_path}" "${class_name}"
 done

}




###############################################################################
#                                     End                                     #
###############################################################################