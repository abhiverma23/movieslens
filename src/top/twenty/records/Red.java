/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package top.twenty.records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
public class Red extends Reducer<Text, IntWritable, Text, NullWritable> {
	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private TreeMap<Double, Text> top20 = new TreeMap< Double, Text>();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {


		String v[] = key.toString().split("\t");
		Double weight = Double.parseDouble(v[1]);
		top20.put(weight, key);

		if (top20.size() > 10) {
			top20.remove(top20.firstKey());
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		for ( Text moviesRating : top20.values() ) {
			context.write(moviesRating, NullWritable.get());
		}
	}
}
