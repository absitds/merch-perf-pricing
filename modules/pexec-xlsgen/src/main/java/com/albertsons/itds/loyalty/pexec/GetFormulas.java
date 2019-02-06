package com.albertsons.itds.loyalty.pexec;

import java.io.File;
import java.util.Iterator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellUtil;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class GetFormulas {

  public static void main(String[] args) throws Exception {
    XSSFWorkbook workbook =
        new XSSFWorkbook(
            new File("/Users/Shingate/Documents/Albertson/Projects/perf-exec/template-vba.xlsx"));
    XSSFSheet sheet = workbook.getSheet("Main Source");
    Row row = CellUtil.getRow(0, sheet);
    Iterator<Cell> cellIterator = row.cellIterator();
    while (cellIterator.hasNext()) {
      Cell cell = (Cell) cellIterator.next();
      CellAddress address = cell.getAddress();
      if (cell.getCellType() == CellType.FORMULA) {
        String cellFormula = cell.getCellFormula();
        String replaced = cellFormula.replace("1", "%1$d");
        replaced = escapeForJava(replaced, false);

        System.out.println(
            "formulas.put(\"" + address.formatAsString() + "\",\"" + replaced + "\");");
      } else {
      }
    }
  }

  public static String escapeForJava(String value, boolean quote) {
    StringBuilder builder = new StringBuilder();
    if (quote) builder.append("\"");
    for (char c : value.toCharArray()) {
      if (c == '\'') builder.append("\\'");
      else if (c == '\"') builder.append("\\\"");
      else builder.append(c);
    }
    if (quote) builder.append("\"");
    return builder.toString();
  }
}
