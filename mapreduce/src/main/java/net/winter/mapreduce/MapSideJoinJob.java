package net.winter.mapreduce;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapSideJoinJob {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf,"MapSideJoinJob");
        job.setJarByClass(MapSideJoinJob.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MapSideJoinJob.MapSideJoinMapper.class);

        job.setOutputKeyClass(NullWritable.get().getClass());
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private Map<String,String> map = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(new File("names")));
            String line = null;

            while((line=reader.readLine())!=null){
                line = line.trim();
                String[] arr = line.split("\t");
                if(arr.length!=2) {
                    context.getCounter("MapSideJoinJob","NAMES_INPUT_FORMAT_ERROR").increment(1);
                }else{
                    context.getCounter("MapSideJoinJob","NAMES_COUNT").increment(1);
                    map.put(arr[0],arr[1]);
                }
            }

            IOUtils.closeQuietly(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("MapSideJoinJob","INPUT_COUNT").increment(1);
            String line = value.toString();

            String[] record = line.split("\\s+");
            if(record.length!=3){
                context.getCounter("MapSideJoinJob","INPUT_ERROR").increment(1);
            }else{
                StringBuilder sb = new StringBuilder();
                sb.append(record[0]);
                sb.append("\t");
                sb.append(map.get(record[0]));
                sb.append("\t");
                sb.append(record[1]);
                sb.append("\t");
                sb.append(record[2]);

                context.write(NullWritable.get(), new Text(sb.toString()));

                context.getCounter("MapSideJoinJob","OUTPUT_COUNT").increment(1);
            }

        }

    }

}
