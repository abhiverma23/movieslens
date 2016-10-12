/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package movies.ratings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
public class Red extends Reducer<IntWritable, Text, IntWritable, Text> {
	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,
			Context arg2) throws IOException, InterruptedException {
		for(Text v : arg1) arg2.write(arg0, v);
	}
}
