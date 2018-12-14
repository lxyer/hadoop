package tem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: lxyer
 * Date: 2018-12-13
 * Time: 11:06
 */
public class TemReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	@Override
	protected void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		Integer max = Integer.MIN_VALUE;
		for(IntWritable value : values){
			max = max > value.get() ? max : value.get();
		}
		context.write(key,new IntWritable(max));
	}
}
