/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 * @version 0.0.0
 */
package most.viewed.movies;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 */
public class Driver {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		if (args.length != 2) {
			System.out.println("USAGES : " +
					"hadoop jar <JARNAME>.jar <DIRIVERCLASSNAME>" +
					" <INPUTFILE> <OUTPUTDIRECTORY> {ENTER}");
			System.exit(2);
		}

		System.out.println("---WELCOME TO MOST VIEWD MOVIES---");

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Most Viewed Movies");

		job.setJarByClass(Driver.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Com.class);
		job.setReducerClass(Red.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}
