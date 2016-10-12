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
		System.out.println("Combiner Code is beeing Executed");
		boolean isValidUser=false;
		String str="";
		for(Text t:arg1){
			if(t.toString().equals("$$VALID$$"))isValidUser=true;
			else{
				str+=t.toString()+":";
			}
		}
		//TODO Check the below code and confirm that logic is correct
		String st[] = str.split(":");
		String s[];
		if(isValidUser){
			for(int i = 0 ; i < st.length; i++){
				s = st[i].split(",");
				if(s.length==2){
					arg2.write( new IntWritable(Integer.parseInt(s[0])), new Text(s[1]));
				}
			}
		}
	}
}
