package net.winter.mapreduce;

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

public class TopTenMaxNumJob {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf,"TopTenMaxNumJob");
        job.setJarByClass(TopTenMaxNumJob.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TopTenMaxNumMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setSortComparatorClass(Comparator.class);

        job.setCombinerClass(TopTenMaxNumReducer.class);

        job.setReducerClass(TopTenMaxNumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class TopTenMaxNumMapper extends Mapper<LongWritable, Text, IntWritable,NullWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(Integer.parseInt(value.toString())),NullWritable.get());
        }
    }


    private static class TopTenMaxNumReducer extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable> {
        private int count = 0;

        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if(count<10) {
                context.write(key, NullWritable.get());
                count++;
            }

        }
    }

    private static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -new IntWritable.Comparator().compare(b1,s1,l1,b2,s2,l2);
        }
    }

}
