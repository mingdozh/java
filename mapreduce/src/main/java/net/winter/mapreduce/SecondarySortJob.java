package net.winter.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SecondarySortJob {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf,"SecondarySortJob");
        job.setJarByClass(SecondarySortJob.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(SecondarySortJob.SortMapper.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class SortMapper extends Mapper<LongWritable, Text, PairWritable,Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(StringUtils.isNotEmpty(value.toString())) {
                String line = value.toString();
                PairWritable k = new PairWritable();
                k.x = new IntWritable(Integer.parseInt(line.split(" ")[0]));
                k.y = new IntWritable(Integer.parseInt(line.split(" ")[1]));

                context.write(k, value);
            }
        }
    }

    public static class SortReducer extends Reducer<PairWritable,Text,NullWritable, Text> {
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text text:values) {
                context.write(NullWritable.get(), text);
            }
        }
    }



}
