/**
 * @author Abhishek Verma
 * @email abhishekverma3210@gmail.com
 * @version 0.0.0
 */
package top.twenty.records;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

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

		System.out.println("---WELCOME TO TOP TWENTY RATED MOVIES---");

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Top 20 Rated Movies : Phase 4 [LAST]");

		job.setNumReduceTasks(0);

		job.setJarByClass(Driver.class);
		job.setMapperClass(Map.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}