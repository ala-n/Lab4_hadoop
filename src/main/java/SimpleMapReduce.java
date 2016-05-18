import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.hsqldb.lib.StringUtil;

import java.io.IOException;

public class SimpleMapReduce {
    public static class SimpleMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(!StringUtil.isEmpty(line)){
                String lineValues[] = line.split("[\t ]");
                try{
                    context.write(
                            new IntWritable(Integer.parseInt(lineValues[0])),
                            new IntWritable(Integer.parseInt(lineValues[1]))
                    );
                }catch (NumberFormatException e){
                    System.err.println(e);
                    System.exit(0);
                }
            }
        }
    }

    public static class SimpleReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for (IntWritable val : values) {
                if(builder.length() > 0 ) {
                    builder.append(",");
                }
                builder.append(val.get());
            }
            context.write(new Text(key.toString()+"\t"), new Text(builder.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "SimpleMapReduce");

        job.setJarByClass(SimpleMapReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}