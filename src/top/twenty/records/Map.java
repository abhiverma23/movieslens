/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package top.twenty.records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
 */
public class Map extends Mapper<Object, Text, Text, IntWritable> {

	private TreeMap<Double, Text> top20 = new TreeMap<Double, Text>();
	private final IntWritable one = new IntWritable(1);
	private IntWritable movieId = new IntWritable();
	
	/**
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// ratings.csv format userId,movieId,rating,timestamp
		String str[] = value.toString().split("\t");
		if(str.length!=2){
			System.out.println("Unwanted data in ratings.csv");
		}
		else{
			try{
				double d = Double.parseDouble(str[1]);
				top20.put(d,value);
				if(top20.size()>10) top20.remove(top20.firstKey());
			}catch (NumberFormatException e) {
				System.out.println("Found Improper movieId => \"" + str[1] + "\"");
			}
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		for ( Text moviesRating : top20.values() ) {
			context.write(moviesRating, null);
		}
	}
}
