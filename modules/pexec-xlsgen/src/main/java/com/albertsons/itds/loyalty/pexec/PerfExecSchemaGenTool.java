package com.albertsons.itds.loyalty.pexec;

import com.albertsons.itds.loyalty.pexec.core.PerfExecSchema;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.poi.ss.util.CellReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tool to generate performance execution tool excels */
public class PerfExecSchemaGenTool {

  private static Logger LOG = LoggerFactory.getLogger(PerfExecSchemaGenTool.class);

  /**
   * Entry point for the tool
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length > 0);
    File schemaFile = new File(args[0]);
    Preconditions.checkArgument(
        schemaFile.exists(), "Invalid input. Teradata schema file does not exists");
    BufferedReader br = new BufferedReader(new FileReader(schemaFile));
    PerfExecSchema schema = new PerfExecSchema();
    Set<String> ignoreAttrs = new TreeSet<>();

    ignoreAttrs.add("division_id");
    ignoreAttrs.add("rpt_group");
    ignoreAttrs.add("row_offset");
    ignoreAttrs.add("t_rank_by_rog_and_cpc");
    ignoreAttrs.add("rog_and_cig");
    ignoreAttrs.add("allowance_counts");
    ignoreAttrs.add("report_date");

    Map<String, String> formulas = new TreeMap<>();
    //    formulas.put(
    //        "new_multiple",
    // "IFERROR(((CW%1$d/MAX(CV%1$d,1))-AZ%1$d)/(CW%1$d/MAX(CV%1$d,1)),\"\")");
    //    formulas.put(
    //        "new_promo_multiple",
    //        "IFERROR(((CZ%1$d/MAX(CY%1$d,1))-CG%1$d)/(CZ%1$d/MAX(CY%1$d,1)),\"\")");
    //    formulas.put(
    //        "new_promo_retail",
    //
    // "IFERROR(IF(ISNUMBER(CZ%1$d),IF(ISNUMBER(CW%1$d),((CW%1$d/MAX(CV%1$d,1))-(CZ%1$d/MAX(CY%1$d,1)))/CE%1$d,((BI%1$d/MAX(BH%1$d,1))-(CZ%1$d/MAX(CY%1$d,1)))/CE%1$d),\"\"),\"\")");
    //    formulas.put("comp_ad_price",
    // "IF(OR(SUM(CW%1$d,CZ%1$d)>0,TRIM(DJ%1$d)<>\"\"),\"Y\",\"\")");
    formulas.put("A1", "IF(AND(CP%1$d>0,CG%1$d=0),\"Y\",\"\")");
    formulas.put("B1", "IF(AND(SUM(BU%1$d,BZ%1$d)>0, CP%1$d=0),\"Y\",\"\")");
    //    formulas.put("C1","\"\"&IFERROR(IF(LEN(W%1$d)>0,INDEX(\'Missing
    // Allowances\'!$C:$C,MATCH(DN%1$d,\'Missing Allowances\'!A:A,0),%1$d),\"\"),\"\")");
    formulas.put("D1", "IF(AX%1$d>0,\"Y\",\"\")");
    formulas.put("F1", "IF(AND(CS%1$d>0,CS%1$d<0.%1$d),\"Y\",\"\")");
    formulas.put("G1", "IF(AND(CH%1$d>0,CH%1$d<0.%1$d),\"Y\",\"\")");
    formulas.put("H1", "IF(CW%1$d>%1$d,\"Y\",\"\")");
    formulas.put("I1", "IF(ROUND((BK%1$d-INT(BK%1$d)),2)=0.09,\"Y\",\"\")");
    formulas.put("J1", "IF(AS%1$d=%1$d,\"Y\",\"\")");
    formulas.put(
        "L1",
        "IF(OR(AC%1$d=2%1$d%1$d30,AC%1$d=79893,AC%1$d=58200,AC%1$d=%1$d%1$d535,AC%1$d=4%1$d303,AC%1$d=4%1$d%1$d30),\"Y\",\"\")");
    //  formulas.put("M1","IFERROR(LEFT(INDEX(\'SEATTLE KVIs\'!$D:$D,MATCH($AD%1$d,\'SEATTLE
    // KVIs\'!$B:$B,0),%1$d),%1$d),\"\")");
    // formulas.put("T1","IFERROR(INDEX(\'SEATTLE
    // PAs\'!$C:$C,MATCH(RIGHT($R%1$d,4)&\"-\"&TEXT($S%1$d,\"00\"),\'SEATTLE
    // PAs\'!$B:$B,0),%1$d),\"Price Area \"&TEXT($S%1$d,\"00\"))");
    //    formulas.put("AO1","IFERROR(MATCH(RIGHT($R%1$d,4)&\"-\"&$AD%1$d,\'TD Query
    // Results\'!$D$%1$d:$D$69,0)-%1$d,\"\")");
    //    formulas.put("AP1","IF(OR($AO%1$d=\"\",$AO%1$d=0),0,IFERROR(INDEX(\'TD Query
    // Results\'!$N:$N,$AO%1$d+%1$d,0),0))");
    //    formulas.put("AQ1","IF(OR($AO%1$d=\"\",$AO%1$d=0),0,IFERROR(INDEX(\'TD Query
    // Results\'!$O:$O,$AO%1$d+%1$d,%1$d),0))");
    formulas.put("AR1", "IFERROR(AP%1$d/AQ%1$d,\"\")");
    //    formulas.put("AT1","IFERROR(INDEX(\'TD Query Results\'!$P:$P,$AO%1$d+%1$d,%1$d)/INDEX(\'TD
    // Query Results\'!$Q:$Q,$AO%1$d+%1$d,%1$d),\"\")");
    //
    // formulas.put("AU1","IF(W%1$d=\"\",SUMIF(CPCSalesAndUnits!A$2:A$69,AD%1$d,CPCSalesAndUnits!C$2:C$69),SUMIF(CPCSalesAndUnits!B$2:B$69,W%1$d,CPCSalesAndUnits!C$2:C$69))");
    //
    // formulas.put("AV1","IF(W%1$d=\"\",SUMIF(CPCSalesAndUnits!A$2:A$69,AD%1$d,CPCSalesAndUnits!D$2:D$69),SUMIF(CPCSalesAndUnits!B$2:B$69,W%1$d,CPCSalesAndUnits!D$2:D$69))");
    formulas.put("AW1", "IFERROR(AU%1$d/AV%1$d,\"\")");
    formulas.put("AZ1", "IFERROR((AX%1$d/BA%1$d)*BK%1$d,\"\")");
    formulas.put("BI1", "IFERROR(((BG%1$d/BF%1$d)-BB%1$d)/(BG%1$d/BF%1$d),\"\")");
    formulas.put("BN1", "IFERROR(((BK%1$d/BJ%1$d)-BB%1$d)/(BK%1$d/BJ%1$d),\"\")");
    formulas.put("CG1", "SUM(BQ%1$d,BV%1$d,BZ%1$d)");
    formulas.put("CH1", "IFERROR(CG%1$d/BA%1$d,\"\")");
    formulas.put("CI1", "BA%1$d-CG%1$d");
    formulas.put("CO1", "IFERROR((CP%1$d-CI%1$d)/CP%1$d,\"\")");
    formulas.put("CS1", "IFERROR(CT%1$d/(BK%1$d/BJ%1$d),\"\")");
    formulas.put("CW1", "IFERROR(CT%1$d/CG%1$d,0)");
    formulas.put(
        "CZ1", "IFERROR(((CY%1$d/MAX(CX%1$d,%1$d))-BB%1$d)/(CY%1$d/MAX(CX%1$d,%1$d)),\"\")");
    formulas.put(
        "DC1", "IFERROR(((DB%1$d/MAX(DA%1$d,%1$d))-CI%1$d)/(DB%1$d/MAX(DA%1$d,%1$d)),\"\")");
    formulas.put(
        "DD1",
        "IFERROR(IF(ISNUMBER(DB%1$d),IF(ISNUMBER(CY%1$d),((CY%1$d/MAX(CX%1$d,%1$d))-(DB%1$d/MAX(DA%1$d,%1$d)))/CG%1$d,((BK%1$d/MAX(BJ%1$d,%1$d))-(DB%1$d/MAX(DA%1$d,%1$d)))/CG%1$d),\"\"),\"\")");
    formulas.put(
        "DI1",
        "IFERROR(IF(DH%1$d>0,((BK%1$d/MAX(BJ%1$d,%1$d))-(DH%1$d/MAX(DG%1$d,%1$d)))/(BK%1$d/MAX(BJ%1$d,%1$d)),\"\"),\"\")");
    formulas.put("DM1", "IF(OR(SUM(CY%1$d,DB%1$d)>0,TRIM(DL%1$d)<>\"\"),\"Y\",\"\")");

    ArrayList<String> list = new ArrayList<>();
    String line = br.readLine();
    int column = -1;
    while (line != null) {
      if (line.indexOf("StructField") != -1) {
        Map<String, String> columnDetails = new TreeMap<>();
        String name = line.substring(line.indexOf('\'') + 1, line.lastIndexOf('\'')).trim();
        String originalType =
            line.substring(line.indexOf("',") + 2, line.lastIndexOf("), ") + 1).trim();
        String type = originalType.substring(0, originalType.indexOf("Type("));
        columnDetails.put("inputType", type);
        if ("Decimal".equals(type)) {
          String digits =
              originalType
                  .substring(originalType.indexOf("(") + 1, originalType.indexOf(","))
                  .trim();
          String decimals =
              originalType
                  .substring(originalType.indexOf(",") + 1, originalType.indexOf(")"))
                  .trim();
          columnDetails.put("digits", digits);
          columnDetails.put("decimals", decimals);
        } else if ("Date".equals(type)) {
          columnDetails.put("inputDateFormat", "yyyy-mm-dd");
          columnDetails.put("outputDateFormat", "mm/dd");
        }
        if (ignoreAttrs.contains(name)) {
          columnDetails.put("ignoreInOutput", "true");
        } else {
          column++;
          columnDetails.put("excelColumnName", CellReference.convertNumToColString(column));
        }
        String colNam = CellReference.convertNumToColString(column) + "1";
        if (!ignoreAttrs.contains(name) && formulas.containsKey(colNam)) {
          columnDetails.put("formula", formulas.get(colNam));
        }
        list.add(name);
        schema.getColumnDetails().put(name, columnDetails);
      }
      line = br.readLine();
    }
    schema.setColumnNames(list.toArray(new String[0]));
    br.close();
    Gson gson = new Gson();
    String json = gson.toJson(schema);
    System.out.println(json);
  }
}
