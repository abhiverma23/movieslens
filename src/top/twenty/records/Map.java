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
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
 */
public class Map extends Mapper<Object, Text, Text, NullWritable> {

	private HashMap<Text, Double> top20 = new HashMap<Text, Double>();
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
				//System.out.println("Curent Values : " + d + " - " + value.toString() + "\n Size : " + top20.size());
				if(top20.size()>10)
				{
					Set<Entry<Text, Double>> set = top20.entrySet();
					List<Entry<Text, Double>> list = new ArrayList<Entry<Text, Double>>(
							set);
					Collections.sort(list, new Comparator<Entry<Text, Double>>() {
						public int compare(Entry<Text, Double> o1,
										   Entry<Text, Double> o2) {
							return o2.getValue().compareTo(o1.getValue());
						}
					});
					/*System.out.println("Removing Line : " + top20.firstKey());*/
					list.remove(10);

					top20 = new HashMap<Text, Double>();
					for (Entry<Text, Double> j : list)
						top20.put(j.getKey(),j.getValue());
				}
				/*System.out.println("Remaning records : " + top20);*/
			}catch (NumberFormatException e) {
				System.out.println("Found Improper movieId => \"" + str[1] + "\"");
			}
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		Set<Entry<Text, Double>> set = top20.entrySet();
		List<Entry<Text, Double>> list = new ArrayList<Entry<Text, Double>>(
				set);
		Collections.sort(list, new Comparator<Entry<Text, Double>>() {
			public int compare(Entry<Text, Double> o1,
							   Entry<Text, Double> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		for (Entry<Text, Double> j : list)
			context.write(j.getKey(),NullWritable.get());
	}
}