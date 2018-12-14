package tem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: lxyer
 * Date: 2018-12-13
 * Time: 10:30
 */
public class TemMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
	@Override
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] arr = value.toString().split(" ");
		context.write(new IntWritable(Integer.parseInt(arr[0])),new IntWritable(Integer.parseInt(arr[1])));
	}
}
