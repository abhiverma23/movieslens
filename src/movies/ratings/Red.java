/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package movies.ratings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
public class Red extends Reducer<IntWritable, Text, IntWritable, Text> {
	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,
			Context arg2) throws IOException, InterruptedException {
		//userId	[timesViewed,$$VALID$$][title]
		boolean isValidUser=false;
		String str="";
		String st="";
		for(Text t:arg1){
			String[] s = t.toString().split(",");
			if(s.length==2){
				if (s[1].equals("$$VALID$$")) {
					isValidUser = true;
					st=s[0];
				}
			}
			else{
				str=t.toString();
			}
		}
		//TODO Check the below code and confirm that logic is correct
		if(isValidUser){
			arg2.write( null, new Text(str+"\t"+arg0.toString()));
		}
	}
}
