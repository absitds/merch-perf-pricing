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
#                           Setup Hive Directories                            #
###############################################################################

#fn_create_hdfs_directory "${DB_RAW_XML_DIR}"
#
#fn_create_hdfs_directory "${DB_RAW_CSV_DIR}"
#
#fn_create_hdfs_directory "${DB_RAW_JSON_DIR}"
#
#fn_create_hdfs_directory "${DB_STAGE_XML_DIR}"
#
#fn_create_hdfs_directory "${DB_STAGE_CSV_DIR}"
#
#fn_create_hdfs_directory "${DB_STAGE_JSON_DIR}"
#
#fn_create_hdfs_directory "${DB_GOLD_XML_DIR}"
#
#fn_create_hdfs_directory "${DB_GOLD_CSV_DIR}"
#
#fn_create_hdfs_directory "${DB_GOLD_JSON_DIR}"

fn_create_hdfs_directory "${DFS_PEXEC_MR_CONFIG_DIR}"

fn_create_hdfs_directory "${DFS_PEXEC_IN_DIR}"

fn_create_hdfs_directory "${DFS_PEXEC_OUT_DIR}"

fn_copy_file_from_local_to_hadoop ${PROJECT_HOME_DIRECTORY}/pexec-xlsgen/etc/divisions.json ${DFS_PEXEC_MR_CONFIG_DIR} ${BOOLEAN_TRUE}

fn_copy_file_from_local_to_hadoop ${PROJECT_HOME_DIRECTORY}/pexec-xlsgen/etc/perf-exec-schema.json ${DFS_PEXEC_MR_CONFIG_DIR} ${BOOLEAN_TRUE}

fn_copy_file_from_local_to_hadoop ${PROJECT_HOME_DIRECTORY}/pexec-xlsgen/etc/pricing-tool-master--v1.02-automation-v0.89-template.xlsm ${DFS_PEXEC_MR_CONFIG_DIR} ${BOOLEAN_TRUE}

################################################################################
#                                     End                                      #
################################################################################