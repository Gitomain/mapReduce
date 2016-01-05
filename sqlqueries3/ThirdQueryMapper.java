package sqlqueries3;
import java.io.IOException; import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ThirdQueryMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private Text outputKey = new Text("");
	private Text outputValue = new Text("");
	List<Integer> lstRequiredAttribList = new ArrayList<Integer>(); 
	StringBuilder strMapValueBuilder = new StringBuilder("");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Select Att to add￼
		lstRequiredAttribList.add(1);
	}
	
	private String buildMapValue(String arrEntityAttributesList[]){
		// This method returns csv list of values to emit
		strMapValueBuilder.setLength(0);// Initialize
		// Build list of attributes to output
		for (int i = 0; i < lstRequiredAttribList.size(); i++) {
			// If the field is in the list of required output
			// append to stringbuilder
			strMapValueBuilder.append(
					arrEntityAttributesList[lstRequiredAttribList.get(i)])
					.append(",");
		}

		if (strMapValueBuilder.length() > 0) {
			strMapValueBuilder.setLength(strMapValueBuilder.length()- 1);
		}			
		return strMapValueBuilder.toString();	
		}
	
	protected void 
	map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		if (value.toString().length() > 0) {
			String arrEntityAttributes[] =
					value.toString().split(",");
			outputValue.set(buildMapValue(arrEntityAttributes));
			if (outputValue.getLength() != 0) {
				outputKey.set(arrEntityAttributes[2]);
				context.write(new
						IntWritable(Integer.parseInt(outputKey.toString())), outputValue);
			}
		}
	}
}
