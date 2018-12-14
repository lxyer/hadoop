package tem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: lxyer
 * Date: 2018-12-13
 * Time: 11:20
 */
public class TemApp{
	public static void main(String[] args) throws Exception{
		if(args.length==0){
			System.out.println("null");
			args = new String[2];
			args[0] = "file:///Volumes/D/bigdata/tem";
			args[1] = "file:///Volumes/D/bigdata/tem/out";
		}
		JobConf jobConf = new JobConf();
		FileInputFormat.addInputPath(jobConf,new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf,new Path(args[1]));
		Job job = Job.getInstance(jobConf);
		job.setJobName("TemApp");
		job.setJarByClass(TemApp.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(TemMapper.class);
		job.setReducerClass(TemReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(4);
		
		job.waitForCompletion(true);
	}
	
}
