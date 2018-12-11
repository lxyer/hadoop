package wc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: lxyer
 * Date: 2018-12-11
 * Time: 14:03
 */
public class WcApp{
	public static void main(String[] args){
		try{
			JobConf config = new JobConf();
//			config.addResource(new Path("file:///Volumes/D/bigdata/wc/"));
			//设置输出路径
			FileOutputFormat.setOutputPath(config,new Path(args[1]));
			Job job = Job.getInstance(config);
			
			// 判断output文件夹是否存在，如果存在则删除
			Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
			FileSystem fileSystem = path.getFileSystem(job.getConfiguration());// 根据path找到这个文件
			if(fileSystem.exists(path)){
				fileSystem.delete(path,true);// true的意思是，就算output有东西，也一带删除
			}
			
			//设置输入路径
			FileInputFormat.addInputPath(job,new Path(args[0]));
			/**
			 * 设置Job属性
			 */
			job.setJobName("WcApp");
			job.setJarByClass(WcApp.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
//			JobConf jobConf = new JobConf(config);
			//jobConf.setOutputFormat(SequenceFileOutputFormat.class);
			job.setMapperClass(WcMapper.class);
			job.setReducerClass(WcReducer.class);
			
			job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.waitForCompletion(true);
		}catch(IOException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}
	}
	
}
