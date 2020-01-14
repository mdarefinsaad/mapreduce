package solutions.assignment2;

import java.io.IOException;
import java.util.StringTokenizer;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

public class MapRedSolution2
{
    /* your code goes in here*/
    /* Map Class */
    public static class TimeMapData extends Mapper<LongWritable, Text, Text, IntWritable>{
    
        //private final static IntWritable one = new IntWritable(1);
        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        String strLine, dataDateTime, dataTime, hour;
        String st[], stTime[], stHour[];
        int hr, x;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        try{
                strLine = value.toString(); // 2,2016-06-09 21:06:36,2016-06-09 21:13:08,2,.79,-73.983360290527344,40.760936737060547,
    
                st = strLine.split(","); // [0]=>2, [1]=>2016-06-09 21:06:36
                
                dataDateTime = st[1]; // 2016-06-09 21:06:36

                stTime = dataDateTime.split(" "); // [0]=>2016-06-09, [1]=>21:06:36

                dataTime = stTime[1]; // 21:06:36

                stHour = dataTime.split(":"); // [0]=>21, [1]=>06, [2]=>36 

                hr = Integer.parseInt(stHour[0]); // 21
 

                if (hr>12) {
                    x = hr-12;
                    hour = Integer.toString(x);
                    String s = hour + "pm";
                    context.write(new Text(s),  new IntWritable(1));
                    //System.out.println(hr + " " + new Text(s));
                }
                else if (hr<=12) {
                    if(hr == 0){
                        hour = Integer.toString(12);
                        String r = hour + "am";
                        context.write(new Text(r),  new IntWritable(1));
                        // System.out.println(hr + " " + new Text(r)); 
                    }
                    else {
                        hour = Integer.toString(hr);
                        String r = hour + "am";
                        context.write(new Text(r),  new IntWritable(1));
                        // System.out.println(hr + " " + new Text(r));     
                    }
                    
                }
                else {
                    context.write(new Text("Missing some values"), new IntWritable(1));
                }    
            }
            catch (Exception e){
                
            }
            

        }
    }


    /* Reduce Class */
    public static class TimeReduceData extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        private IntWritable outcome = new IntWritable();
        int sum;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            sum = 0;
            for (IntWritable x : values){
                sum = sum + x.get();
            }
            outcome.set(sum);
            context.write(key, outcome);
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #2");
        
        /* your code goes in here*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TimeMapData.class);
        job.setReducerClass(TimeReduceData.class);


        

        /*********************************/
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83", "07f6514a2f48cff8e12fdbc533bc0fe5", 
            "e3c247d186e3f7d7ba5bab626a8474d7", "fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd", 
            "6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466", "7d35ce45afd621e46840627a79f87dac"};
        
        for (String validMd5 : validMd5Sums) 
        {
            if (validMd5.contentEquals(md5))
            {
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}
