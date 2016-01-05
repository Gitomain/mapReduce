package sqlqueries3; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.WritableComparator; 
public class ThirdQuerySortingComparator extends WritableComparator { 
	
	protected ThirdQuerySortingComparator() { 
		super(IntWritable.class, true); 
	} 

	@Override 
	public int compare(WritableComparable w1, WritableComparable w2) { 
		// Sort on all attributes of composite key
		IntWritable key1 = (IntWritable) w1; IntWritable key2 = (IntWritable) w2; 
		int cmpResult = key1.compareTo(key2);  
		return cmpResult;   
	} 
}
