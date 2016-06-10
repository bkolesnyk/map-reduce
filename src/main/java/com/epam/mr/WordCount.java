package com.epam.main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, CustomKey, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private CustomKey customKey = new CustomKey();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                customKey.setText(itr.nextToken());
                context.write(customKey, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<CustomKey,IntWritable,CustomKey,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(CustomKey key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class CustomPartitioner extends Partitioner<Text, IntWritable> {
        public int getPartition(Text text, IntWritable value, int numReduceTasks) {
            String[] str = value.toString().split("\t");
            int age = Integer.parseInt(str[2]);

            if (numReduceTasks == 0) {
                return 0;
            }

            if (age <= 20) {
                return 0;
            } else if (age > 20 && age <= 30) {
                return 1 % numReduceTasks;
            } else {
                return 2 % numReduceTasks;
            }
        }

        public void configure(JobConf jobConf) {
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(Text.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text k1 = (Text)w1;
            Text k2 = (Text)w2;
            return k1.compareTo(k2);
        }
    }

    public static class CustomKey implements WritableComparable{
        private String text;
        private int number;

        public CustomKey(){
            this.text = "";
            this.number = 0;
        }

        public int compareTo(Object o) {
            return this.text.compareTo(((CustomKey)o).text);
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + text.hashCode();
            result = prime * result + (int) (number ^ (number >>> 32));
            return result;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(text);
            dataOutput.writeInt(number);
        }

        public void readFields(DataInput dataInput) throws IOException {
            text = dataInput.readUTF();
            number = dataInput.readInt();
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setGroupingComparatorClass(KeyComparator.class);
        job.setSortComparatorClass(KeyComparator.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(CustomKey.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
