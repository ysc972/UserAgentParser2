package com.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ParseUserAgentApp {

    //Map类实现
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable write = new LongWritable(1);
        private UserAgentParser userAgentParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //每条日志信息
            String information = value.toString();

            String informationSource = information.substring(getCharacterPosition(information, "\"", 7) + 1);
            UserAgent agent = userAgentParser.parse(informationSource);
            String brower = agent.getBrowser();
            context.write(new Text(brower), write);
            String os=agent.getOs();
            context.write(new Text(os),write);
            String engine=agent.getEngine();
            context.write(new Text(engine),write);
            String platform=agent.getPlatform();
            context.write(new Text(platform),write);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
        }
    }

    //Reduce类实现
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (LongWritable value: values){
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));

        }
    }

    /**
     * 获取指定字符串中指定标识的字符串出现的索引位置
     * @param value 指定的字符串
     * @param operator 指定标识
     * @param index 索引位置
     * @return 返回的索引位置
     */
    private static int getCharacterPosition(String value, String operator, int index){

        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int matcherIndex = 0;
        while (slashMatcher.find()) {
            matcherIndex++;

            if (matcherIndex == index) {
                break;
            }
        }
        return slashMatcher.start();
    }


    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();

        // 若输出路径有内容，则先删除
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("路径存在，但已被删除");
        }

        Job job = Job.getInstance(configuration, "ParseUserAgentApp");

        job.setJarByClass(ParseUserAgentApp.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
