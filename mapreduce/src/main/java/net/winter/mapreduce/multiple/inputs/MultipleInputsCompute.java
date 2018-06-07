package net.winter.mapreduce.multiple.inputs;

import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class MultipleInputsCompute {

	public static void main(String[] args) throws Exception {
		Configuration conf = new JobConf();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] arguments = optionParser.getRemainingArgs();

		Validate.isTrue(arguments.length == 3, "argument counts should be 3");

		Job job = Job.getInstance(conf);
		job.setJarByClass(MultipleInputsCompute.class);		
	
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setReducerClass(MultipleInputsReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		MultipleInputs.addInputPath(job, new Path(arguments[0]), TextInputFormat.class, OnlyOneNumMapper.class);
		MultipleInputs.addInputPath(job, new Path(arguments[1]), TextInputFormat.class, MultipleNumMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(arguments[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
	

	private static class OnlyOneNumMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

		private final LongWritable ONE = new LongWritable(1);

		protected void setup(Context context) throws IOException, InterruptedException {
			System.err.println("setup OnlyOneNumMapper");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			Scanner in = new Scanner(line);
			LongWritable out = new LongWritable();
			if (in.hasNextLong()) {
				out.set(in.nextLong());
			}
			context.write(ONE, out);
			in.close();

		}
	}

	private static class MultipleNumMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

		private final LongWritable ONE = new LongWritable(1);

		protected void setup(Context context) throws IOException, InterruptedException {
			System.err.println("setup MultipleNumMapper");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			Scanner in = new Scanner(line);
			LongWritable out = new LongWritable();
			while (in.hasNextLong()) {
				out.set(in.nextLong());
				context.write(ONE, out);
			}
			in.close();
		}
	}

	private static class MultipleInputsReducer extends Reducer<LongWritable, LongWritable, NullWritable, LongWritable> {

		protected void setup(Context context) throws IOException, InterruptedException {
			System.err.println("setup MutipleInputReducer");
		}

		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long max = Long.MIN_VALUE;
			for (LongWritable value : values) {
				if (value.get() > max) {
					max = value.get();
				}
			}

			context.write(NullWritable.get(), new LongWritable(max));
		}
	}

}
