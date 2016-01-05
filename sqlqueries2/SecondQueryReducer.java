package sqlqueries2;

import java.io .IOException;
import java. util . Iterator ;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondQueryReducer extends Reducer<Text, Text, NullWritable, Text> { 
	private Text outputValue = new Text("");
	private NullWritable outputKey = NullWritable.get();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		StringBuilder strBuilder = new StringBuilder("");
		Integer count = 0;
		
		for (Text val : values) { 
			String arrEntityAttributes[] = val.toString().split(",");
			if(arrEntityAttributes[0].length()!=0)
				count++;
		}
		strBuilder.append(key).append(",").append(count.toString());
		outputValue.set(strBuilder.toString());; 
		context.write(outputKey, outputValue);
	}
}