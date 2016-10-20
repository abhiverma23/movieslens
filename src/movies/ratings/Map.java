/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package movies.ratings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
 */
public class Map extends Mapper<Object, Text, IntWritable, Text> {

	private IntWritable movieId = new IntWritable();

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// File1[MostViewedMovies] format : moviesId	timesViewed
		// File2[movies.csv] format : movieId,title,genres
		String str1[] = value.toString().split("\t");//Length = 2
		String str[] = value.toString().split(",");//Length = 3
		//TODO Check the below code and confirm that logic is correct
		if(str1.length==2){
			try{
				movieId.set(Integer.parseInt(str1[0]));
				context.write(movieId,new Text(str1[1]+",$$VALID$$"));//movieId	timesViewed,$$VALID$$
			}catch (NumberFormatException e) {
				System.out.println("Found Improper movieId in movies.csv file => \"" + str1[0] + "\"");
			}
		}
		else if(str.length==3){
			try{
				movieId.set(Integer.parseInt(str[0]));
				context.write(movieId, new Text(str[1]));//movieId	title
			}catch(NumberFormatException e){
				System.out.println("Found Improper movieId MostViewedMovies file => \"" + str[0] + "\"");
			}
		}
		else{
			System.out.println("Not a valid Record  => "+value.toString());
		}
	}
}
