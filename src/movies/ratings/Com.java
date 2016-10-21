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
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		//movieId	[rating][rating][$$VALID$$][rating] ...
		System.out.println("Reducer Code is beeing Executed");
		boolean isValidUser=false;
		String str="";
		for(Text t:values){
			if(t.toString().equals("$$VALID$$"))isValidUser=true;
			else{
				//rating:rating ...
				str+=t.toString()+":";
			}
		}
		//TODO Check the below code and confirm that logic is correct
		String st[] = str.split(":");
		String s[];
		if(isValidUser){
			for(int i = 0 ; i < st.length; i++){
				//movieId	rating
				context.write( key, new Text(st[i]));
			}
		}
	}
}
