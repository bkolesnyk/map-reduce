package com.epam.mr.task1;

import com.epam.mr.examples.JoinRecordWithStationName;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 *
 */
public class Task1 extends Configured implements Tool {
    public static class TextPair2 implements WritableComparable<TextPair2> {
        private Text date;
        private Text tag;
        private LongWritable arrDelay;

        TextPair2() {
            this.date = new Text();
            this.tag = new Text();
            this.arrDelay = new LongWritable();
        }

        TextPair2(Text date, Text tag, LongWritable arrDelay) {
            this.date = date;
            this.tag = tag;
            this.arrDelay = arrDelay;
        }

        @Override
        public int compareTo(TextPair2 o) {
            int cmp = date.compareTo(o.getDate());
            if (cmp != 0) {
                return cmp;
            }
            return tag.compareTo(o.getTag());
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            date.write(dataOutput);
            tag.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            date.readFields(dataInput);
            tag.readFields(dataInput);
        }

        public Text getDate() {
            return date;
        }

        public void setDate(Text date) {
            this.date = date;
        }

        public Text getTag() {
            return tag;
        }

        public void setTag(Text tag) {
            this.tag = tag;
        }

        public LongWritable getArrDelay() {
            return arrDelay;
        }

        public void setArrDelay(LongWritable arrDelay) {
            this.arrDelay = arrDelay;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TextPair2 textPair2 = (TextPair2) o;

            if (date != null ? !date.equals(textPair2.date) : textPair2.date != null) {
                return false;
            }
            if (tag != null ? !tag.equals(textPair2.tag) : textPair2.tag != null) {
                return false;
            }
            return arrDelay != null ? arrDelay.equals(textPair2.arrDelay) : textPair2.arrDelay == null;

        }

        @Override
        public int hashCode() {
            int result = date != null ? date.hashCode() : 0;
            result = 31 * result + (tag != null ? tag.hashCode() : 0);
            result = 31 * result + (arrDelay != null ? arrDelay.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "TextPair2{" +
                    "date=" + date +
                    ", tag=" + tag +
                    ", arrDelay=" + arrDelay +
                    '}';
        }
    }

    public static class FlightMapper extends Mapper<LongWritable, Text, TextPair2, Text> {
        private Text date = new Text();
        private LongWritable arrDelay = new LongWritable();
        private Text info = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split(",");

            if (input[17].equals("SFO") && !input[14].equals("NA")) {
                date.set(input[0] + "," + input[1] + "," + input[2]);
                arrDelay.set(Integer.parseInt(input[14]));
                info.set(new StringBuilder()
                        .append(input[4]).append(",")
                        .append(input[6]).append(",")
                        .append(input[8]).append(",")
                        .append(input[9]).append(",")
                        .append(input[11]).append(",")
                        .append(input[14]).append(",")
                        .append(input[15]).append(",")
                        .append(input[16]).append(",")
                        .append(input[17]).toString()
                );
                context.write(new TextPair2(date, new Text("0"), arrDelay), info);
            }
        }
    }

    public static class WeatherMapper extends Mapper<LongWritable, Text, TextPair2, Text> {
        private Text date = new Text();
        private LongWritable arrDelay = new LongWritable(0);
        private Text info = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split(",");
            date.set(input[1] + "," + input[2] + "," + input[3]);
            info.set(new StringBuilder()
                    .append(input[4]).append(",")
                    .append(input[5]).append(",")
                    .append(input[6]).toString());
            context.write(new TextPair2(date, new Text("1"), arrDelay), info);
        }
    }

    public static class Task1Reducer extends Reducer<TextPair2, Text, Text, Text> {
        @Override
        protected void reduce(TextPair2 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            Text flight = new Text(iter.next());
            while (iter.hasNext()) {
                Text weather = iter.next();
                Text outValue = new Text(flight.toString() + "," + weather.toString());
                context.write(key.getDate(), outValue);
            }
        }
    }

    public static class KeyPartitioner extends Partitioner<TextPair2, Text> {
        @Override
        public int getPartition(TextPair2 key, Text value, int numPartitions) {
            return (key.getDate().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(TextPair2.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return ((TextPair2) w1).getDate().compareTo(((TextPair2) w2).getDate());
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextPair2.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            int cmp = ((TextPair2) w1).getDate().compareTo(((TextPair2) w2).getDate());
            if (cmp != 0) {
                return cmp;
            }
            int cmp2 = ((TextPair2) w1).getArrDelay().compareTo(((TextPair2) w2).getArrDelay());
            if (cmp2 != 0) {
                return cmp2;
            }
            return ((TextPair2) w1).getTag().compareTo(((TextPair2) w2).getTag()); //reverse
        }
    }

    public int run(String[] args) throws Exception {
//        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (args.length != 3) {
            System.out.println("Usage <flights input> <weather input> <output>");
            return -1;
        }
        Job job = new Job(getConf(), "Join flights records with weather");
        job.setJarByClass(getClass());
        Path flightsInputPath = new Path(args[0]);
        Path weatherInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job, flightsInputPath, TextInputFormat.class, FlightMapper.class);
        MultipleInputs.addInputPath(job, weatherInputPath, TextInputFormat.class, WeatherMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setSortComparatorClass(KeyComparator.class);

        job.setMapOutputKeyClass(TextPair2.class);

        job.setReducerClass(Task1Reducer.class);

        job.setOutputKeyClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Task1(), args);
        System.exit(exitCode);
    }
}
