package com.epam.mr.examples;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;
import java.util.List;

/**
 *
 *
 */
public class MapSideJoinTwoSetsDriver {
//    public static class SortByKeyMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//        private int keyIndex;
//        private Splitter splitter;
//        private Joiner joiner;
//        private Text joinKey = new Text();
//
//
//        @Override
//        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
//            String separator =  context.getConfiguration().get("separator");
//            keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex"));
//            splitter = Splitter.on(separator);
//            joiner = Joiner.on(separator);
//        }
//
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            Iterable<String> values = splitter.split(value.toString());
//            joinKey.set(Iterables.get(values,keyIndex));
//            if(keyIndex != 0){
//                value.set(reorderValue(values,keyIndex));
//            }
//            context.write(joinKey,value);
//        }
//
//
//        private String reorderValue(Iterable<String> value, int index){
//            List<String> temp = Lists.newArrayList(value);
//            String originalFirst = temp.get(0);
//            String newFirst = temp.get(index);
//            temp.set(0,newFirst);
//            temp.set(index,originalFirst);
//            return joiner.join(temp);
//        }
//    }
//
//    public static class SortByKeyReducer extends Reducer<Text,Text,NullWritable,Text> {
//
//        private static final NullWritable nullKey = NullWritable.get();
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            for (Text value : values) {
//                context.write(nullKey,value);
//            }
//        }
//    }
//
//    private static Configuration getMapJoinConfiguration(String separator, String... paths) {
//        Configuration config = new Configuration();
//        config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", separator);
//        String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, paths);
//        config.set("mapred.join.expr", joinExpression);
//        config.set("separator", separator);
//        return config;
//    }
//
//    public static class CombineValuesMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {
//
//        private static final NullWritable nullKey = NullWritable.get();
//        private Text outValue = new Text();
//        private StringBuilder valueBuilder = new StringBuilder();
//        private String separator;
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            separator = context.getConfiguration().get("separator");
//        }
//
//        @Override
//        protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
//            valueBuilder.append(key).append(separator);
//            for (Writable writable : value) {
//                valueBuilder.append(writable.toString()).append(separator);
//            }
//            valueBuilder.setLength(valueBuilder.length() - 1);
//            outValue.set(valueBuilder.toString());
//            context.write(nullKey, outValue);
//            valueBuilder.setLength(0);
//        }
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        String separator = ",";
//        String keyIndex = "0";
//        int numReducers = 10;
//        String jobOneInputPath = args[0];
//        String jobTwoInputPath = args[1];
//        String joinJobOutPath = args[2];
//
//        String jobOneSortedPath = jobOneInputPath + "_sorted";
//        String jobTwoSortedPath = jobTwoInputPath + "_sorted";
//
//        Job firstSort = Job.getInstance(getConfiguration(keyIndex, separator));
//        configureJob(firstSort, "firstSort", numReducers, jobOneInputPath, jobOneSortedPath, SortByKeyMapper.class, SortByKeyReducer.class);
//
//        Job secondSort = Job.getInstance(getConfiguration(keyIndex, separator));
//        configureJob(secondSort, "secondSort", numReducers, jobTwoInputPath, jobTwoSortedPath, SortByKeyMapper.class, SortByKeyReducer.class);
//
//        Job mapJoin = Job.getInstance(getMapJoinConfiguration(separator, jobOneSortedPath, jobTwoSortedPath));
//        configureJob(mapJoin, "mapJoin", 0, jobOneSortedPath + "," + jobTwoSortedPath, joinJobOutPath, CombineValuesMapper.class, Reducer.class);
//        mapJoin.setInputFormatClass(CompositeInputFormat.class);
//
//        List<Job> jobs = Lists.newArrayList(firstSort, secondSort, mapJoin);
//        int exitStatus = 0;
//        for (Job job : jobs) {
//            boolean jobSuccessful = job.waitForCompletion(true);
//            if (!jobSuccessful) {
//                System.out.println("Error with job " + job.getJobName() + "  " + job.getStatus().getFailureInfo());
//                exitStatus = 1;
//                break;
//            }
//        }
//        System.exit(exitStatus);
//    }
}
