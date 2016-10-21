/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 */
package last.five.year.most.watched.movies;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Abhishek Verma
 * email abhishekverma3210@gmail.com
 *
 */
public class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
	
	
	private final IntWritable one = new IntWritable(1);
	private IntWritable movieId = new IntWritable();
	
	/**
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// movies.csv format movieId,title,genres
		String str[] = value.toString().split(",");
		if(str.length!=3){
			System.out.println("Unwanted data in movies.csv");
		}
		else{
			try{
				movieId.set(Integer.parseInt(str[0]));
				String s[] = str[1].split("\\(");
                s = s[s.length-1].split("\\)");
                try{
                    int year = Integer.parseInt(s[0]);
                    if(year>=2011) {
                        one.set(year);
                        context.write(movieId, one);//MovieId	year
                    }
                }catch (NumberFormatException e) {
                    System.out.println("No number found in End of string with MovieID inside parentheses "
                            + movieId.get());
                }catch (IndexOutOfBoundsException e){
                    System.out.println("There is no patter found to split parentheses ArrayOutOfBound MovieID: "
                            + movieId.get());
                }
			}catch (NumberFormatException e) {
				System.out.println("Found Improper movieId => \"" + str[1] + "\"");
			}
		}
	}
}