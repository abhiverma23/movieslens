/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package calculate.ratings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 */
public class Map extends Mapper<Object, Text, IntWritable, Text> {
	
	private IntWritable userId = new IntWritable();
	
	/**
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//movieId	rating

		String[] row = value.toString().split("\t");
		if(row.length!=2){
			System.out.println("Unwanted data in validMoviesRating file");
		}
		//movieId	rating,1
		context.write(new IntWritable(Integer.parseInt(row[0])),new Text(row[1].toString()+",1"));
	}
}