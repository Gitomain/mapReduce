package reducesidejoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DriverReduceSideJoin extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Three parameters are required for DriverReduceSideJoin - <input dir1> <input dir2> <output dir>\n");
			return -1;
		}
		//Job configuration
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("ReduceSideJoin");
		job.setJarByClass(DriverReduceSideJoin.class);
		
		//Add side data to distributed cache
		DistributedCache.addCacheFile(new URI("/user/liwang/input_data"), conf);
		conf.setInt("vins.data", 2);//Set the vins data file to 2
		conf.setInt("achats.data", 1);//Set Achats dataa file to 1
		conf.setInt("achats10.data", 1);//Set Achats dataa file to 1
		conf.setInt("achats100.data", 1);//Set Achats dataa file to 1
		conf.setInt("achats1000.data", 1);//Set Achats dataa file to 1
		
		//Build csv lsit of input files
		StringBuilder inputPaths = new StringBuilder();
		inputPaths.append(args[0].toString()).append(",").append(args[1].toString());
		
		//Configure remaining aspects of the job
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(args[2]));		
		
		job.setMapperClass(MapperReduceSideJoin.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(4);
		job.setReducerClass(ReducerReduceSideJoin.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new DriverReduceSideJoin(), args);
		System.exit(exitCode);
	}
}
