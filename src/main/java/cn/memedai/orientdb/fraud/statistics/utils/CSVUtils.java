package cn.memedai.orientdb.fraud.statistics.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hangyu on 2017/5/23.
 */
public class CSVUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtils.class);

    public static File createFileAndColName(String filePath, String fileName, String[] colNames){
        deleteFile(filePath, fileName);
        File csvFile = new File(filePath, fileName);
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(csvFile, "UTF-8");
            StringBuffer sb = new StringBuffer();
            for(int i=0; i<colNames.length; i++){
                if( i<colNames.length-1 )
                    sb.append(colNames[i]+",");
                else
                    sb.append(colNames[i]+"\r\n");

            }
            pw.print(sb.toString());
            pw.flush();
            pw.close();
            return csvFile;
        } catch (Exception e) {
            LOGGER.error("createFileAndColName has e is {}", e);
        }
        return null;
    }

    /**
     * 删除单个文件
     * @param filePath
     *     文件目录路径
     * @param fileName
     *     文件名称
     */
    public static void deleteFile(String filePath, String fileName) {
        File file = new File(filePath);
        if (file.exists()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isFile()) {
                    if (files[i].getName().equals(fileName)) {
                        files[i].delete();
                        return;
                    }
                }
            }
        }
    }

    public static boolean appendDate(File csvFile, List<List<String>> data){
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile, true), "UTF-8"), 1024);
            for(int i=0; i<data.size(); i++){
                List tempData = data.get(i);
                StringBuffer sb = new StringBuffer();
                for(int j=0; j<tempData.size(); j++){
                    if(j<tempData.size()-1)
                        sb.append(tempData.get(j)+",");
                    else
                        sb.append(tempData.get(j)+"\r\n");
                }
                bw.write(sb.toString());
                if(i%1000==0)
                    bw.flush();
            }
            bw.flush();
            bw.close();

            return true;
        } catch (Exception e) {
            LOGGER.error("appendDate has e is {}", e);
        }
        return false;
    }


    public static void main(String[] args){
        File csvFile = new File("E:/temp", "test.csv");

        //String[] colNames = {"第一列","第二列","第三列","第四列"};
        //File csvFile = createFileAndColName("E:/temp", "test.csv", colNames);
        List<String> list = new ArrayList<String>();
        list.add("1234567899632564dadas"+"\t");
        list.add("1234567899632564"+"\t");
        String s = null;
        list.add("");
        list.add("2107-05-09"+"\t");
        /*List<String> list2 = new ArrayList<String>();
        list2.add("12x");
        list2.add("22x");
        list2.add("32x");
        list2.add("42x");*/
        List<List<String>> data = new ArrayList<List<String>>();
        data.add(list);
        //data.add(list2);
       // appendDate(csvFile, data);
        appendDate(csvFile, data);




        System.out.println( System.getProperty("file.encoding") );
    }
}
