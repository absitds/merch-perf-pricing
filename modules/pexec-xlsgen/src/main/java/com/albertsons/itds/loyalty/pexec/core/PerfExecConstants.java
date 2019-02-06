package com.albertsons.itds.loyalty.pexec.core;

import java.text.NumberFormat;

public abstract class PerfExecConstants {

  public static final int ZERO_DIVISIONS = 0;

  public static final String EMPTY_STRING = "";

  public static final String COLUMN_DELIMITER = "|";

  public static final String GROUP_ID_COLUMN_NAME = "group_cd";

  public static final String DIVISION_ID_COLUMN_NAME = "division_id";

  public static final String REPORT_GROUP_COLUMN_NAME = "rpt_group";

  public static final String PEXEC_DEFAULT_XML = "perf-exec-default.xml";

  public static final String PEXEC_SITE_XML = "perf-exec-site.xml";

  public static final int INDEX_OF_MAIN_SHEET = 0;

  public static final String REPORT_GROUP_DELIMITER = "^";

  public static final String COUNTER_GROUP_ALB_PERF_EXEC = "alb-perf-exec";

  public static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  public static final String XLSX_FILE_EXTENSION = ".xlsm";

  // Configuration Keys
  public static final String PER_EXEC_TEMPLATE_FILE_PATH = "itds.perf.exec.template.file.path";

  public static final String PER_EXEC_DIVISIONS_FILE_PATH = "itds.perf.exec.divisions.file.path";

  public static final String PER_EXEC_SCHEMA_FILE_PATH = "itds.perf.exec.schema.file.path";

  public static final String PERF_EXEC_REPORT_DATE = "itds.perf.exec.report.date";
}
