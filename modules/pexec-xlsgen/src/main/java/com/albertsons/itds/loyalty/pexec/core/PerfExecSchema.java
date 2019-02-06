package com.albertsons.itds.loyalty.pexec.core;

import java.util.LinkedHashMap;
import java.util.Map;

public class PerfExecSchema {

  private LinkedHashMap<String, Map<String, String>> columnDetails = new LinkedHashMap<>();

  private String[] columnNames;

  private transient int indexOfDivisionId = -1;

  private transient int indexOfGroupId = -1;

  private transient int indexOfReportGroup = -1;

  /** @return the indexOfDivisionId */
  public int getIndexOfDivisionId() {
    return indexOfDivisionId;
  }

  /** @return the indexOfGroupId */
  public int getIndexOfGroupId() {
    return indexOfGroupId;
  }

  public int getIndexOfReportGroup() {
    return indexOfReportGroup;
  }

  /** @param indexOfDivisionId the indexOfDivisionId to set */
  public void setIndexOfDivisionId(int indexOfDivisionId) {
    this.indexOfDivisionId = indexOfDivisionId;
  }

  /** @param indexOfGroupId the indexOfGroupId to set */
  public void setIndexOfGroupId(int indexOfGroupId) {
    this.indexOfGroupId = indexOfGroupId;
  }

  public void setIndexOfReportGroup(int indexOfReportGroup) {
    this.indexOfReportGroup = indexOfReportGroup;
  }

  public Map<String, String> getColumnDetails(String columnName) {
    return columnDetails.get(columnName);
  }

  public void setColumnDetails(LinkedHashMap<String, Map<String, String>> columnDetails) {
    this.columnDetails = columnDetails;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(String[] columnNames) {
    this.columnNames = columnNames;
  }

  public LinkedHashMap<String, Map<String, String>> getColumnDetails() {
    return columnDetails;
  }
}
