package com.daniel.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * @Author BRIAN
 * @Description 第二个mapreduce job——对第一次的结果进行迭代
 **/
public class UnitSum {

    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
            从HDFS读取第一次的结果，\t是写入HDFS的默认分隔符，mapper主要还是原样输出
            输入:key   subPR(每一个权重)
                 2  1/4*1/6012
            输出:key = 2 value = 1/4*1/6012
             */
            String[] pageSubRank = value.toString().trim().split("\t");
            double subRank = Double.parseDouble(pageSubRank[1]);
            context.write(new Text(pageSubRank[0]), new DoubleWritable(subRank));
        }

    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            /*
            Reduce对map的结果进行聚合
            输入:key = 2 (到达页) values = <1/4*1/6012, 1/9*6012, ...> (所有mapper的集合即到达页的总权重)
            输出:key = 2 value = sum
             */
            double total = 0;
            for (DoubleWritable value : values) {
                total += value.get();
            }
            // 保留5位小数
            DecimalFormat df = new DecimalFormat("#.00000");
            total = Double.valueOf(df.format(total));
            context.write(key, new DoubleWritable(total));

        }
    }

    public void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);
        job.setMapperClass(PassMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        // 为两个mapper设置不同的路径
        // args[0] =subPR(第一个mr输出，第二个mr的输入)
        // args[1] =pr(第二个mr的输出，第一个mr的第二个mapper的输入)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }

}
