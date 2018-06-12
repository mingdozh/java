package net.winter.mapreduce.sort;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortCompute {
	private static enum SortComputeCounterEnum {
		INPUT_DATA_ERROR;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();

		System.err.println(Arrays.toString(remainingArgs));

		Job job = Job.getInstance(conf, "SortCompute");

		job.setJarByClass(SortCompute.class);

		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(SortReducer.class);
		job.setNumReduceTasks(2);

		job.setPartitionerClass(LetterPartitioner.class);

		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	private static class LetterPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return "ABCDEFGHIJKLMNOPQRSTUVWXYZ".indexOf(key.toString().charAt(0)) % numPartitions;
		}

	}

	private static class SortComparator extends WritableComparator {
		@SuppressWarnings("unused")
		protected SortComparator() {
			super(Text.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			Text ta = (Text) a;
			Text tb = (Text) b;
			return tb.toString().compareTo(ta.toString());
		}
	}

	private static class GroupComparator extends WritableComparator {
		@SuppressWarnings("unused")
		protected GroupComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text ta = (Text) a;
			Text tb = (Text) b;
			return ta.charAt(0) - tb.charAt(0);
		}
	}

	private static class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] words = line.split("\\s+");

			if (words == null || words.length != 3) {
				context.getCounter(SortComputeCounterEnum.INPUT_DATA_ERROR).increment(1);
				return;
			}

			context.write(new Text(words[0]), new Text(words[0] + "\t" + words[2]));

		}
	}

	private static class SortReducer extends Reducer<Text, Text, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext()) {
				Text text = iterator.next();
				String[] words = text.toString().split("\\s+");
				sum += Integer.parseInt(words[1]);
			}

			context.write(NullWritable.get(), new Text(key.toString().charAt(0) + "\t" + String.valueOf(sum)));
		}
	}

}
