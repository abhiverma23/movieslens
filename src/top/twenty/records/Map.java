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
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
 */
public class Map extends Mapper<Object, Text, Text, NullWritable> {

	private static HashMap<String, Double> top20 = new HashMap<String, Double>();
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
				top20.put(value.toString(),d);
				System.out.println("Curent Values : " + d + " - " + value.toString() + "\n Size : " + top20.size());
				if(top20.size()>10)
				{
					Set<Entry<String, Double>> set = top20.entrySet();
					List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(
							set);
					Collections.sort(list, new Comparator<Entry<String, Double>>() {
						public int compare(Entry<String, Double> o1,
										   Entry<String, Double> o2) {
							return o2.getValue().compareTo(o1.getValue());
						}
					});
					/*System.out.println("Removing Line : " + top20.firstKey());*/
					/*System.out.println(list.get(10));
					System.out.println(list.remove(10));*/

					top20.clear();
					int i = 0;
					for (Entry<String, Double> j : list)
						if(i++<20) {
							System.out.println("After Removing : " + j.getKey() + j.getValue());
							top20.put(j.getKey(), j.getValue());
						}
				}
			}catch (NumberFormatException e) {
				System.out.println("Found Improper movieId => \"" + str[1] + "\"");
			}
			System.out.println("Remaning records : " + top20);
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		Set<Entry<String, Double>> set = top20.entrySet();
		List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(
				set);
		Collections.sort(list, new Comparator<Entry<String, Double>>() {
			public int compare(Entry<String, Double> o1,
							   Entry<String, Double> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		for (Entry<String, Double> j : list) {
			System.out.println("Final Output : " + j.getKey() + " -- " + NullWritable.get());
			context.write(new Text(j.getKey()), NullWritable.get());
		}
	}
}