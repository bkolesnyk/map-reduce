package com.epam.mr.task1;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 *
 */
public class Task1 extends Configured implements Tool {
    public static class TextPair2 implements WritableComparable<TextPair2> {
        private Text date;
        private LongWritable arrDelay;

        TextPair2() {
            this.date = new Text();
            this.arrDelay = new LongWritable();
        }

        TextPair2(Text date, LongWritable arrDelay) {
            this.date = date;
            this.arrDelay = arrDelay;
        }

        @Override
        public int compareTo(TextPair2 o) {
            int cmp = date.compareTo(o.getDate());
            if (cmp != 0) {
                return cmp;
            }
            return arrDelay.compareTo(o.getArrDelay());
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            date.write(dataOutput);
            arrDelay.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            date.readFields(dataInput);
            arrDelay.readFields(dataInput);
        }

        public Text getDate() {
            return date;
        }

        public void setDate(Text date) {
            this.date = date;
        }


        public LongWritable getArrDelay() {
            return arrDelay;
        }

        public void setArrDelay(LongWritable arrDelay) {
            this.arrDelay = arrDelay;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TextPair2 textPair2 = (TextPair2) o;

            return new EqualsBuilder()
                    .append(date, textPair2.date)
                    .append(arrDelay, textPair2.arrDelay)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(date)
                    .append(arrDelay)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return date + "," + arrDelay;
        }

    }

    public static class FlightMapper extends Mapper<LongWritable, Text, TextPair2, Text> {
        private Text date = new Text();
        private LongWritable arrDelay = new LongWritable();
        private Text info = new Text();
        private TextPair2 textPair2 = new TextPair2();
        private static HashMap<String, String> weatherMap = new HashMap<String, String>();
        private BufferedReader brReader;

        enum MYCOUNTER {
            RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR, MAP_SIZE
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().trim().equals("sfo_weather.csv")) {
                    context.getCounter(FlightMapper.MYCOUNTER.FILE_EXISTS).increment(1);
                    loadWeatherMap(eachPath, context);
                }
            }

        }

        private void loadWeatherMap(Path path, Context context) throws IOException {
            String strLineRead = "";
            try {
                brReader = new BufferedReader(new FileReader(path.toString()));

                // Read each line, split and load to HashMap
                while ((strLineRead = brReader.readLine()) != null) {
                    String weatherFieldArray[] = strLineRead.split(",");
                    String date = weatherFieldArray[1] + "," + transformDate(weatherFieldArray[2]) + "," + transformDate(weatherFieldArray[3]);
                    String info = weatherFieldArray[4] + "," + weatherFieldArray[5] + "," + weatherFieldArray[6];
                    weatherMap.put(date, info);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                context.getCounter(FlightMapper.MYCOUNTER.FILE_NOT_FOUND).increment(1);
            } catch (IOException e) {
                context.getCounter(FlightMapper.MYCOUNTER.SOME_OTHER_ERROR).increment(1);
                e.printStackTrace();
            } finally {
                if (brReader != null) {
                    brReader.close();
                }
            }
            context.getCounter(MYCOUNTER.MAP_SIZE).increment(weatherMap.size());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(FlightMapper.MYCOUNTER.RECORD_COUNT).increment(1);
            String[] input = value.toString().split(",");
            if (input[17].equals("SFO") && !input[14].equals("NA")) {
                String d = input[0] + "," + transformDate(input[1]) + "," + transformDate(input[2]);
                String weather = weatherMap.get(d);
                date.set(d);
                arrDelay.set(Integer.parseInt(input[14]));
                textPair2.setDate(date);
                textPair2.setArrDelay(arrDelay);
                info.set(new StringBuilder()
                        .append(input[4]).append(",")
                        .append(input[6]).append(",")
                        .append(input[8]).append(",")
                        .append(input[9]).append(",")
                        .append(input[11]).append(",")
                        .append(input[14]).append(",")
                        .append(input[15]).append(",")
                        .append(input[16]).append(",")
                        .append(input[17]).append(",")
                        .append(weather).toString()
                );
                context.write(textPair2, info);
            }
        }

        private String transformDate(String date) {
            String result = date;
            if (date.length() == 1) {
                result = "0" + date;
            }
            return result;
        }
    }


    public static class Task1Reducer extends Reducer<TextPair2, Text, Text, Text> {
        @Override
        protected void reduce(TextPair2 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                Text info = iter.next();
                context.write(new Text(key.getDate().toString()), info);
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
            return -1 * ((TextPair2) w1).getArrDelay().compareTo(((TextPair2) w2).getArrDelay());
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage <flights input> <weather input> <output>");
            return -1;
        }
        Job job = new Job(getConf(), "Join flights records with weather");
        job.setJarByClass(getClass());

        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");


        Path flightsInputPath = new Path(args[0]);
        String weatherInputPath = args[1];
        Path outputPath = new Path(args[2]);

        DistributedCache.addCacheFile(new URI(weatherInputPath), conf);

        job.setMapperClass(FlightMapper.class);

        FileInputFormat.setInputPaths(job, flightsInputPath);
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
