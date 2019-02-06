package com.albertsons.itds.loyalty.pexec.core;

import com.albertsons.itds.loyalty.pexec.core.PerfExecDivisionDetails.Division;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PerfExecHelper {

  private static PerfExecHelper helper = new PerfExecHelper();

  public static PerfExecHelper getInstance() {
    return helper;
  }

  private PerfExecDivisionDetails divisionDetails;

  private PerfExecSchema schema;

  public PerfExecDivisionDetails getDivisionDetails(Configuration conf) throws IOException {
    if (divisionDetails == null) {
      synchronized (this) {
        if (divisionDetails == null) {
          Path divisionsFilePath = getDivisionsFilePath(conf);
          FileSystem fileSystem = divisionsFilePath.getFileSystem(conf);
          FSDataInputStream fsDataInputStream = fileSystem.open(divisionsFilePath);
          divisionDetails =
              new Gson()
                  .fromJson(
                      new InputStreamReader(fsDataInputStream), PerfExecDivisionDetails.class);
          Collection<Division> divisions = divisionDetails.getDivisions().values();
          for (Division division : divisions) {
            Preconditions.checkArgument(!StringUtils.isEmpty(division.getName()));
          }
        }
      }
    }
    return divisionDetails;
  }

  public Path getDivisionsFilePath(Configuration conf) {
    return new Path(conf.get(PerfExecConstants.PER_EXEC_DIVISIONS_FILE_PATH));
  }

  public PerfExecSchema getSchema(Configuration conf) throws IOException {
    if (schema == null) {
      synchronized (this) {
        if (schema == null) {
          Path schemaFilePath = getSchemaFilePath(conf);
          FileSystem fileSystem = schemaFilePath.getFileSystem(conf);
          FSDataInputStream fsDataInputStream = fileSystem.open(schemaFilePath);
          schema =
              new Gson().fromJson(new InputStreamReader(fsDataInputStream), PerfExecSchema.class);
          String[] columnNames = schema.getColumnNames();
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            if (PerfExecConstants.GROUP_ID_COLUMN_NAME.equals(columnName)) {
              schema.setIndexOfGroupId(i);
            } else if (PerfExecConstants.DIVISION_ID_COLUMN_NAME.equals(columnName)) {
              schema.setIndexOfDivisionId(i);
            } else if (PerfExecConstants.REPORT_GROUP_COLUMN_NAME.equals(columnName)) {
              schema.setIndexOfReportGroup(i);
            }
          }
          Preconditions.checkArgument(schema.getIndexOfDivisionId() != -1);
          Preconditions.checkArgument(schema.getIndexOfGroupId() != -1);
          Preconditions.checkArgument(schema.getIndexOfReportGroup() != -1);
        }
      }
    }
    return schema;
  }

  public Path getSchemaFilePath(Configuration conf) {
    return new Path(conf.get(PerfExecConstants.PER_EXEC_SCHEMA_FILE_PATH));
  }

  public Path getTemplateFilePath(Configuration conf) {
    return new Path(conf.get(PerfExecConstants.PER_EXEC_TEMPLATE_FILE_PATH));
  }
}
