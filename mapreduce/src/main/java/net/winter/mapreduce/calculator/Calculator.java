package net.winter.mapreduce.calculator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class Calculator {
	private enum CalculatorCounter {
		PROCESSED_LINE;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new JobConf();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		System.err.println(Arrays.toString(remainingArgs));

		System.err.println("env=" + optionParser.getConfiguration().get("env"));

		Job job = Job.getInstance(conf, "calculator");
		job.setJarByClass(Calculator.class);
		job.setMapperClass(CalculatorMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class CalculatorMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Scanner input = new Scanner(value.toString().trim());

			int sum = 0;
			while (input.hasNextInt()) {
				sum = sum + input.nextInt();
			}

			IOUtils.closeQuietly(input);

			context.write(NullWritable.get(), new Text(String.valueOf(sum)));

			context.getCounter(CalculatorCounter.PROCESSED_LINE).increment(1);
		}
	}

}
