/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package calculate.ratings;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		double R = 0;
		long T = 0;
		for(Text rt : values){
			String [] s = rt.toString().split(",");
			R += Double.parseDouble(s[0]);
			T += Integer.parseInt(s[1]);
		}
		double RT = R/T;
		//movieId	rating
		context.write(key,new Text(RT+""));
	}
}
