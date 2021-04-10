package com.daniel.mr;

import java.io.*;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author BRIAN
 * @Description 过滤并取出topN的值，这里我取前100
 **/
public class FilterFile {
    public static void main(String[] args) throws IOException {
        int topN = 100;
        InputStreamReader inr = new InputStreamReader(new FileInputStream("src/main/java/com/daniel/mr/part-r-00000"));
        BufferedReader bf = new BufferedReader(inr);
        String fileName;
        // 按行读取字符串
        Map<String, String> map = new HashMap<>();
        while ((fileName = bf.readLine()) != null) {
            String[] split = fileName.split("\t");
            if (!"0.0".equals(split[1])) {
                BigDecimal db = new BigDecimal(split[1]);
                String ii = db.toPlainString();
//                FilterFile.saveAsFileWriter(split[0] + "\t" + ii + "\r\n", "src/main/java/pagerank/all_result.txt", true);//将所有值保存为文件
                map.put(split[0], ii);
            }

        }
        // 使用stream对map进行排序
        LinkedHashMap<String, String> linkedMap = new LinkedHashMap<>();
        map.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .forEachOrdered(entry -> linkedMap.put(entry.getKey(), entry.getValue()));
        int count = 0;
        for (String key : linkedMap.keySet()) {
            count++;
            String values = linkedMap.get(key);
            System.out.println(key + "\t" + values);
            if (count == topN)
                break;
        }
    }

    public static void saveAsFileWriter(String content, String fileName, boolean append) {
        FileWriter fwriter = null;
        try {
            fwriter = new FileWriter(fileName, append);
            fwriter.write(content);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                fwriter.flush();
                fwriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
