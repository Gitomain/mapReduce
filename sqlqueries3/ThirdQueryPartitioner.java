package sqlqueries3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ThirdQueryPartitioner extends Partitioner<IntWritable, Text> { 
	@Override 
	public int getPartition(IntWritable key, Text value, int numReduceTasks) { 
		return (key.hashCode() % numReduceTasks); 
	}
}
