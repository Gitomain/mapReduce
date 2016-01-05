package sqlqueries1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FirstQueryDriver extends Configured implements Tool { 

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Two parameters are required <inputdir1> <output dir>\n");
			return -1;
		}
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("First Query");
		job.setJarByClass(FirstQueryDriver.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(FirstQueryMapper.class);
		job.setNumReduceTasks(0);
		boolean success = job.waitForCompletion(true);
		return (success? 0:1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new FirstQueryDriver(), args);
		System.exit(exitCode);
	}
}