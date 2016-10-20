/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package filter.users;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
 */
public class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
	
	
	private final IntWritable one = new IntWritable(1);
	private IntWritable userId = new IntWritable();
	
	/**
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// ratings.csv format userId,userId,rating,timestamp
		String str[] = value.toString().split(",");
		if(str.length!=4){
			System.out.println("Unwanted data in ratings.csv");
		}
		else{
			try{
				userId.set(Integer.parseInt(str[0]));
				context.write(userId, one);
			}catch (NumberFormatException e) {
				System.out.println("Found Improper userId => \"" + str[0] + "\"");
			}
		}
	}
}
