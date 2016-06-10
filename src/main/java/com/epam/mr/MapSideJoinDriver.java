package com.epam.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 *
 */
public class MapSideJoinDriver extends Configured implements Tool
{
    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static HashMap<String, String> CustIdOrderMap = new HashMap<String, String>();
        private BufferedReader brReader;
        private String orderNO = "";
        private Text outKey = new Text("");
        private Text outValue = new Text("");

        enum MYCOUNTER {
            RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
        }

        @Override
        protected void setup(Context context) throws IOException,
                                                     InterruptedException {

            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context
                    .getConfiguration());

            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("order_custid.txt")) {
                    context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
                    setupOrderHashMap(eachPath, context);
                }
            }

        }

        private void setupOrderHashMap(Path filePath, Context context)
                throws IOException {

            String strLineRead = "";

            try {
                brReader = new BufferedReader(new FileReader(filePath.toString()));

                while ((strLineRead = brReader.readLine()) != null) {
                    String custIdOrderArr[] = strLineRead.toString().split("\\s+");
                    CustIdOrderMap.put(custIdOrderArr[0].trim(),        custIdOrderArr[1].trim());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
            } catch (IOException e) {
                context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
                e.printStackTrace();
            }finally {
                if (brReader != null) {
                    brReader.close();

                }

            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

            if (value.toString().length() > 0) {
                String custDataArr[] = value.toString().split("\\s+");

                try {
                    orderNO = CustIdOrderMap.get(custDataArr[0].toString());
                } finally {
                    orderNO = ((orderNO.equals(null) || orderNO
                            .equals("")) ? "NOT-FOUND" : orderNO);
                }

                outKey.set(custDataArr[0].toString());

                outValue.set(custDataArr[1].toString() + "\t"
                        + custDataArr[2].toString() + "\t"
                        + custDataArr[3].toString() + "\t" + orderNO);

            }
            context.write(outKey, outValue);
            orderNO = "";
        }
    }

    public static void main( String[] args ) throws Exception
    {
        int exitCode = ToolRunner.run(new Configuration(),new MapSideJoinDriver(), args);
        System.exit(exitCode);
    }
    @Override
    public int run(String[] args) throws Exception {
        if(args.length !=2 ){
            System.err.println("Usage : App -files <location-to-cust-id-and-order-file> <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job(getConf());
        job.setJobName("Map Side Join");
        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job,new Path(args[0]) );
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapSideJoinMapper.class);
        job.setNumReduceTasks(0);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;

    }
}
