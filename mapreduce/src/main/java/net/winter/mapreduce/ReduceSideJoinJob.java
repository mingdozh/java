package net.winter.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;


public class ReduceSideJoinJob {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf,"ReduceSideJoinJob");
        job.setJarByClass(ReduceSideJoinJob.class);

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(PairPartitioner.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setReducerClass(ReduceSideJoinReducer.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, NameMapper.class );
        MultipleInputs.addInputPath(job,new Path(otherArgs[1]) ,TextInputFormat.class, FileMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class PairPartitioner extends Partitioner<PairWritable, Text> {
        public int getPartition(PairWritable intWritablePair, Text text, int i) {
            return intWritablePair.x.get()%i;
        }
    }

    public static class SortComparator implements RawComparator<PairWritable> {

        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            PairWritable a = new PairWritable();
            PairWritable b = new PairWritable();

            DataInputBuffer input = new DataInputBuffer();
            try {
                input.reset(bytes, i, i1);
                a.readFields(input);
                input.reset(bytes1,i2,i3);
                b.readFields(input);

                return a.x.get() == b.x.get()? a.y.get() - b.y.get() : a.x.get() - b.x.get();

            }catch(Exception e){
                throw new RuntimeException("cath Exception", e);
            }
        }

        public int compare(PairWritable o1, PairWritable o2) {
            return o1.x.compareTo(o2.x);
        }
    }

    public static class GroupingComparator implements RawComparator<PairWritable> {

        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            PairWritable a = new PairWritable();
            PairWritable b = new PairWritable();

            DataInputBuffer input = new DataInputBuffer();
            try {
                input.reset(bytes, i, i1);
                a.readFields(input);
                input.reset(bytes1,i2,i3);
                b.readFields(input);

                return a.x.get() - b.x.get();
            }catch(Exception e){
                throw new RuntimeException("cath Exception", e);
            }
        }

        public int compare(PairWritable o1, PairWritable o2) {
            return o1.x.compareTo(o2.x);
        }
    }

    public static class NameMapper extends Mapper<LongWritable, Text, PairWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(StringUtils.isNotEmpty(value.toString())) {
                String line = value.toString();
                PairWritable pairWritable = new PairWritable();
                pairWritable.x = new IntWritable(Integer.parseInt(line.split("\\s+")[0]));
                pairWritable.y = new IntWritable(0);

                context.write(pairWritable, value);

                context.getCounter("ReduceSideJoin", "NAME_INPUT_COUNT").increment(1);
            }
        }

    }

    public static class FileMapper extends Mapper<LongWritable, Text, PairWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] record = line.split("\\s+");
            if(record.length!=3){
                context.getCounter("ReduceSideJoin","FILE_Mapper_INPUT_ERROR").increment(1);
            }else{
                PairWritable pairWritable = new PairWritable();
                pairWritable.y = new IntWritable(1);
                pairWritable.x = new IntWritable(Integer.parseInt(record[0]));

                context.write(pairWritable, value);

                context.getCounter("ReduceSideJoin","FILE_OUTPUT_COUNT").increment(1);
            }
        }

    }

    public static class ReduceSideJoinReducer extends Reducer<PairWritable, Text, NullWritable, Text> {

        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Iterator<Text> iterator = values.iterator();

            Text text = iterator.next();

            String line = text.toString();
            String[] arr = line.trim().split("\\s+");

            if(arr.length == 2) {

                context.getCounter("ReduceSidJoin", String.valueOf(arr.length)).increment(1);
                String name = arr[1];

                while(iterator.hasNext()) {
                    text = iterator.next();
                    line = text.toString();
                    line = line.trim() + "\t" + name;

                    context.write(NullWritable.get(),new Text(line));

                }

            }

        }
    }

}
