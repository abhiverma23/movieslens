/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package filter.users;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
public class Red extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1,
			Context arg2) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable v : arg1){
			sum+=v.get();
		}
		if(sum>=40)
			arg2.write(arg0, new IntWritable(sum));
	}
}
