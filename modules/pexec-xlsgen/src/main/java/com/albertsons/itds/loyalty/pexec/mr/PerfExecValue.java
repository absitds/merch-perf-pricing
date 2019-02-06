package com.albertsons.itds.loyalty.pexec.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PerfExecValue implements Writable {

  private String[] columnValues;

  public String[] getColumnValues() {
    return columnValues;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    columnValues = WritableUtils.readStringArray(in);
  }

  public void setColumnValues(String[] columnValues) {
    this.columnValues = columnValues;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeStringArray(out, columnValues);
  }
}
