package com.daniel.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author BRIAN
 * @Description 第一个mapreduce job——计算Transition Matrix * PR Matrix
 **/
public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
            输入:1   2,8,9,24
            输出:key = 1 value=1/4
             */
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");
            // fromTo至少有2个元素，但是也有一个页面不指向任何其他页面的可能性
            if (fromTo.length < 2 || fromTo[1].trim().equals("")) {
                return;
            }
            String from = fromTo[0];
            String[] to = fromTo[1].split(",");
            for (String cur : to) {
                context.write(new Text(from), new Text(cur + "=" + (double) 1 / to.length));
            }
        }

    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
            假设1的值为1/6012
            输入:  pr.txt -> 1 1/6012 （表示key为1的页面的pr值为1/6012）
            输出:  key = 1 value = 1/6012
             */
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }

    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*
            第一次mapper:
            输入: key = 1 (起始页) values = <2=1/4, 7=1/4, 1/6012> (到达的页面 + 起始页的权重)
            输出: key = 2 (到达页) value = 1/4*1/6012 (到达页的部分权重)
            第二次mapper and reduce:
            输入: key = 2 (终止页) values = <1/4*1/6012, 1/9*6012, ...> (到达页权重的总和)
            输出: key = 2 value = sum
             */
            List<String> transitionCells = new ArrayList<String>();
            double prCell = .0;

            // 计算pr
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionCells.add(value.toString().trim());
                } else {
                    prCell = Double.parseDouble(value.toString().trim());
                }
            }

            // 相乘，写出
            for (String cell : transitionCells) {
                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("=")[1]);
                String outputValue = String.valueOf(relation * prCell);
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }

    }

    public void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 为两个mapper设置不同的路径
        // args[0] =transition.txt
        // args[1] =pr.txt
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);
        // 输出文件
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

    }

}