package sqlqueries3; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.util.ToolRunner; 

public class ThirdQueryDriver extends Configured implements Tool { 
	public int run(String[] args) throws Exception { 
		if (args.length != 2) { 
			System.out.printf("Two parameters are required <input dir1> <output dir>\n"); 
			return -1; 
		} 
		Job job = new Job(getConf()); 
		Configuration conf = job.getConfiguration(); 
		job.setJobName("Third Query"); 
		job.setJarByClass(ThirdQueryDriver.class); 
		FileInputFormat.setInputPaths(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		job.setMapperClass(ThirdQueryMapper.class); 
		job.setMapOutputKeyClass(IntWritable.class); 
		job.setMapOutputValueClass(Text.class); 
		job.setPartitionerClass(ThirdQueryPartitioner.class); 
		job.setSortComparatorClass(ThirdQuerySortingComparator.class); 
		job.setGroupingComparatorClass(ThirdQueryGroupingComparator.class); 
		job.setNumReduceTasks(4); 
		//job.setReducerClass(ThirdQueryReducer.class); 
		job.setOutputKeyClass(NullWritable.class); 
		job.setOutputValueClass(Text.class); 	
		boolean success = job.waitForCompletion(true); 
		return success ? 0 : 1; 
	} 

	public static void main(String[] args) throws Exception { 
		int exitCode = ToolRunner.run(new Configuration(), new ThirdQueryDriver(), args); 
		System.exit(exitCode); 
	} 
}
