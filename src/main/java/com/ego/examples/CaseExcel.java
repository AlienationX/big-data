package com.ego.examples;

import jodd.util.StringUtil;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

public class CaseExcel {
    private static final String SEP = "\t";

    public static void main(String[] args) throws Exception {
        // String filename = "E:\\Codes\\Java\\big-data\\data\\table.xlsx";
        String filename = "data/table.xlsx";
        FileInputStream fi = new FileInputStream(filename);
        Workbook wb;
        if (filename.endsWith(".xlsx")) {
            wb = new XSSFWorkbook(fi);
        } else if (filename.endsWith(".xls")) {
            wb = new HSSFWorkbook(fi);
        } else {
            throw new Exception("input file must be Excel");
        }

        Sheet sheet = wb.getSheetAt(0);

        int firstRowNum = sheet.getFirstRowNum();
        int lastRowNum = sheet.getLastRowNum();
        System.out.println(firstRowNum);

        for (int i = 0; i < lastRowNum; i++) {//遍历每一行
            Row row = sheet.getRow(i);
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(i));
            // System.out.println(row.getCell(0));
            int firstCellNum = row.getFirstCellNum();
            int lastCellNum = row.getLastCellNum();
            for (int j = firstCellNum; j < lastCellNum; j++) {
                // if (cell != null) 可以过滤为null的单元格避免空指针错误
                Cell cell = row.getCell(j);
                if (i >= 1 && j == 0) {
                    System.out.println(cell.getCellType());
                    System.out.println(cell.getDateCellValue());
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd").format(cell.getDateCellValue()));
                }
                list.add(cell.toString());
            }
            System.out.println(StringUtil.join(list, SEP));
        }
    }
}
