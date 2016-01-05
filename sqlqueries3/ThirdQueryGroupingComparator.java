package sqlqueries3; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.WritableComparator; 

public class ThirdQueryGroupingComparator extends WritableComparator { 

	protected ThirdQueryGroupingComparator() { 
		super(IntWritable.class, true);   
	} 

	@Override 
	public int compare(WritableComparable w1, WritableComparable w2) { 
		IntWritable key1 = (IntWritable) w1; IntWritable key2 = (IntWritable) w2; 
		return key1.compareTo(key2); 
	} 
}
