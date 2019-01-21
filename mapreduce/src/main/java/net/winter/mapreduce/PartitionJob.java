package net.winter.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class PartitionJob {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf,"PartitionJob");
        job.setJarByClass(PartitionJob.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(PartitionJob.PartitionMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setPartitionerClass(OddEvenPartitioner.class);

        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setNumReduceTasks(3);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class PartitionMapper extends Mapper<LongWritable, Text, IntWritable,NullWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(StringUtils.isNotEmpty(value.toString())) {
                context.write(new IntWritable(Integer.parseInt(value.toString())), NullWritable.get());
            }
        }
    }


    public static class OddEvenPartitioner extends Partitioner<IntWritable,NullWritable> {

        public int getPartition(IntWritable intWritable, NullWritable nullWritable, int i) {
            int n = intWritable.get();

            return n%i;
        }
    }


}
