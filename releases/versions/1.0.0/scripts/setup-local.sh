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
#                           Setup Local Directories                           #
###############################################################################
fn_create_local_directory  "${WORKFLOW_TMP_DIR}/clean-up"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/setup"

fn_create_local_directory  "${BATCH_ID_DATA_DIR}"

#fn_create_local_directory  "${LAST_EXTRACTED_DATE_DIR}"
#
#fn_create_local_directory  "${WORKFLOW_TMP_DIR}/ingest-csv"
#
#fn_create_local_directory  "${WORKFLOW_TMP_DIR}/ingest-json"
#
#fn_create_local_directory  "${WORKFLOW_TMP_DIR}/ingest-xml"
#
#fn_create_local_directory  "${WORKFLOW_TMP_DIR}/transform-csv"
#
#fn_create_local_directory  "${WORKFLOW_TMP_DIR}/transform-json"
#
#fn_create_local_directory  "${WORKFLOW_TMP_DIR}/transform-xml"

fn_copy_local_file "${PROJECT_HOME_DIRECTORY}/pexec-extr/bin/dw_aaai_012_lod_perf_exec" "${PEXEC_SCRIPT_PATH}"

fn_copy_local_file "${PROJECT_HOME_DIRECTORY}/pexec-extr/bin/dw_aaai_012_lod_perf_exec.ksh" "${PEXEC_SCRIPT_PATH}"

fn_create_local_directory  "${LOCAL_ROOT_CONFIGS_PATH}"

fn_create_local_directory  "${LOCAL_SECRETS_PATH}"

fn_create_local_directory  "${LOCAL_STORAGE_ROOT_DIR}"

fn_create_local_directory  "${LOCAL_PEXEC_IN_DIR}"

fn_create_local_directory  "${LOCAL_PEXEC_OUT_DIR}"

fn_create_local_directory  "${PEXEC_EXTR_TEMP_DATA_DIR}"

################################################################################
#                                     End                                      #
################################################################################