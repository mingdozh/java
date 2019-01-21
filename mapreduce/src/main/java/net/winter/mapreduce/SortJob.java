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

public class SortJob {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf,"SortJob");
        job.setJarByClass(SortJob.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(SortJob.SortMapper.class);

        job.setSortComparatorClass(Comparator.class);

        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setNumReduceTasks(3);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class SortMapper extends Mapper<LongWritable, Text, IntWritable,NullWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(StringUtils.isNotEmpty(value.toString())) {
                context.write(new IntWritable(Integer.parseInt(value.toString())), NullWritable.get());
            }
        }
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            DataInputBuffer input = new DataInputBuffer();
            IntWritable a = new IntWritable();
            IntWritable b = new IntWritable();

            try {
                input.reset(b1,s1,l1);
                a.readFields(input);
                input.reset(b2,s2,l2);
                b.readFields(input);

                return a.get() - b.get();
            }catch(Exception e){
                throw new RuntimeException("catch Exception",e);
            }
        }
    }
}
