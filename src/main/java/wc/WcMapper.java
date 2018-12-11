package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * WcMapper类
 * @author: lxyer
 * Date: 2018-12-11
 * Time: 13:36
 */
public class WcMapper extends Mapper<LongWritable,Text, Text,IntWritable>{
	@Override
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		Text keyOut = new Text();
		IntWritable count = new IntWritable();
		String[] strings = value.toString().trim().split("\\W");
		for(String string : strings){
			keyOut.set(string);
			count.set(1);
			context.write(keyOut,count);
		}
	}
}
