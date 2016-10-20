/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 * @version 0.0.0
 */
package movies.ratings;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

		System.out.println("---JOIN MOVIES NAMES WITH TOP 10 MOVIES---");

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Most Viewed Movies : Phase 3 [LAST]");
		
		job.setNumReduceTasks(1);

		job.setJarByClass(Driver.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Red.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(false) ? 0 : 1);
		System.out.println("---MOST VIEWED MOVIES : THE END---");
	}
}
