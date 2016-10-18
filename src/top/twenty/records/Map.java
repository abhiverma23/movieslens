/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package top.twenty.records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
 */
public class Map extends Mapper<Object, Text, Text, NullWritable> {

	private TreeMap<Text, Double> top20 = new TreeMap<Text, Double>();
	private final IntWritable one = new IntWritable(1);
	private IntWritable movieId = new IntWritable();
	
	/**
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String str[] = value.toString().split("\t");
		if(str.length!=2){
			System.out.println("Unwanted data in ratings.csv");
		}
		else{
			try{
				double d = Double.parseDouble(str[1]);
				top20.put(value,d);
				System.out.println("Curent Values : " + d + " - " + value.toString() + "\n Size : " + top20.size());
				if(top20.size()>10)
				{
					System.out.println("Removing Line : " + top20.firstKey());
					top20.remove(top20.firstKey());
				}
				System.out.println("Remaning records : " + top20);
			}catch (NumberFormatException e) {
				System.out.println("Found Improper movieId => \"" + str[1] + "\"");
			}
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		for ( Text moviesRating : top20.keySet() ) {
			context.write(moviesRating, NullWritable.get());
		}
	}
}