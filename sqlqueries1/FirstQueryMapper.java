package sqlqueries1;

import java.io.IOException; import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstQueryMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private NullWritable nullWritableKey = NullWritable.get();
	private LongWritable outputKey = new LongWritable(0);
	private Text outputValue = new Text("");
	List<Integer> lstRequiredAttribList = new ArrayList<Integer>();
	List<Integer> lstCondAttribList = new ArrayList<Integer>();
	StringBuilder strMapValueBuilder = new StringBuilder("");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//Select Att to add
		lstRequiredAttribList.add(1);
		//Cond Att
		lstCondAttribList.add(2);		
	}

	private String buildMapValue(String arrEntityAttributesList[]) {
		// This method returns csv list of values to emit
		strMapValueBuilder.setLength(0);// Initialize		
		if(Integer.parseInt(arrEntityAttributesList[lstCondAttribList.get(0)])==1980){
			// Build list of attributes to output
			for (int i = 0; i < lstRequiredAttribList.size(); i++) {
				// If the field is in the list of required output
				// append to stringbuilder
				strMapValueBuilder.append(arrEntityAttributesList[lstRequiredAttribList.get(i)]).append(",");
			}
		}
		if (strMapValueBuilder.length() > 0) {
		// Drop last comma
		strMapValueBuilder.setLength(strMapValueBuilder.length()- 1);
		}
		return strMapValueBuilder.toString();
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
	throws IOException, InterruptedException {
		if (value.toString().length() > 0) {
			String arrEntityAttributes[] = value.toString().split(",");
			outputValue.set(buildMapValue(arrEntityAttributes));
			if(outputValue.getLength() !=0){
				outputKey.set(Long.parseLong(arrEntityAttributes[0]));
				StringBuilder t = new StringBuilder();
				t.append(outputKey.toString()).append(",").append(outputValue);
				outputValue.set(t.toString());
				context.write(nullWritableKey,outputValue);
			}
		}
	}
}
	