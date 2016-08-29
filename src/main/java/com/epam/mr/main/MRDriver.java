package com.epam.mr.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Bohdan on 8/12/2016.
 */
public class MRDriver {

    public static class MRMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final static Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while(stringTokenizer.hasMoreTokens()){
                word.set(stringTokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class MRReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class MRPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text text, IntWritable intWritable, int i) {
            return (text.toString().hashCode() & Integer.MAX_VALUE) % i;
        }

//        public int getPartition(Text text, IntWritable value, int i) {
//            String[] str = value.toString().split("\t");
//            int age = Integer.parseInt(str[2]);
//
//            if (numReduceTasks == 0) {
//                return 0;
//            }
//
//            if (age <= 20) {
//                return 0;
//            } else if (age > 20 && age <= 30) {
//                return 1 % numReduceTasks;
//            } else {
//                return 2 % numReduceTasks;
//            }
//        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mr driver");
        job.setJarByClass(MRDriver.class);

        job.setMapperClass(MRMapper.class);
        job.setCombinerClass(MRReducer.class);
        job.setReducerClass(MRReducer.class);

        job.setPartitionerClass(MRPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
