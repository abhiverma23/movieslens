/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package movies.ratings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
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
		// ratings.csv format userId,movieId,rating,timestamp
		 	
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		String str[] = value.toString().split(",");
		//TODO Check the below code and confirm that logic is correct
		if(fileName.equals("ratings.csv")){
			if(str.length!=4){
				System.out.println("Unwanted data in ratings.csv");
			}
			else{
				try{
					userId.set(Integer.parseInt(str[0]));
					context.write(userId,
							new Text(str[1]
							+ "," + str[2]));
				}catch (NumberFormatException e) {
					System.out.println("Found Improper movieId => \"" + str[0] + "\"");
				}
			}
		}
		else{
			if(str.length!=2){
				System.out.println("Unwanted data in Valid Users list");
			}
			else{
				try{
					userId.set(Integer.parseInt(str[0]));
					context.write(userId, new Text("$$VALID$$"));
				}catch(NumberFormatException e){
					System.out.println("Found Improper movieId => \"" + str[0] + "\"");
				}
			}
		}
	}
}
