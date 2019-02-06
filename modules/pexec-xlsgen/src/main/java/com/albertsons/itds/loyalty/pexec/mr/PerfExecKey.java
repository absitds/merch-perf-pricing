package com.albertsons.itds.loyalty.pexec.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PerfExecKey implements WritableComparable<PerfExecKey> {

  private String divisionId;

  private String reportGroup;

  public PerfExecKey() {}

  public static PerfExecKey copy(PerfExecKey other) {
    PerfExecKey copy = new PerfExecKey();
    copy.divisionId = other.divisionId;
    copy.reportGroup = other.reportGroup;
    return copy;
  }

  @Override
  public int compareTo(PerfExecKey other) {
    String thisKey = divisionId + "#" + reportGroup;
    String otherKey = other.divisionId + "#" + other.reportGroup;
    return thisKey.compareTo(otherKey);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof PerfExecKey)) {
      return false;
    }
    PerfExecKey other = (PerfExecKey) obj;
    if (divisionId == null) {
      if (other.divisionId != null) {
        return false;
      }
    } else if (!divisionId.equals(other.divisionId)) {
      return false;
    }
    if (reportGroup == null) {
      if (other.reportGroup != null) {
        return false;
      }
    } else if (!reportGroup.equals(other.reportGroup)) {
      return false;
    }
    return true;
  }

  public String getDivisionId() {
    return divisionId;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int prime = 31;
    int result = 1;
    result = prime * result + ((divisionId == null) ? 0 : divisionId.hashCode());
    result = prime * result + ((reportGroup == null) ? 0 : reportGroup.hashCode());
    return result;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    divisionId = WritableUtils.readString(in);
    reportGroup = WritableUtils.readString(in);
  }

  /** @param divisionId the divisionId to set */
  public void setDivisionId(String divisionId) {
    this.divisionId = divisionId;
  }

  /** @param reportGroup the reportGroup to set */
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, divisionId);
    WritableUtils.writeString(out, reportGroup);
  }

  public String getReportGroup() {
    return reportGroup;
  }

  public void setReportGroup(String reportGroup) {
    this.reportGroup = reportGroup;
  }
}
