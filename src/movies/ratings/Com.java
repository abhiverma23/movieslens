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
public class Com extends Reducer<IntWritable, Text, IntWritable, Text> {
	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,
			Context arg2) throws IOException, InterruptedException {
		System.out.println("Reducer Code is being Executed");
		boolean isValidUser=false;
		String str="";
		for(Text t:arg1){
			if(t.toString().equals("$$VALID$$")) {
				isValidUser = true;
			}
			else{
				str+=t.toString()+":";
			}
		}
		String[] st = str.split(":");
		if(isValidUser){
			for (String aSt : st) {
				arg2.write(arg0, new Text(aSt));//moviesID	ratings
			}
		}
	}
}