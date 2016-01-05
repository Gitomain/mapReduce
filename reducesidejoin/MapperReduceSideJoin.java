package reducesidejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperReduceSideJoin extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text outputKey = new Text("");
	private Text outputValue = new Text("");
	int intSrcIndex = 0;
	List<Integer> lstRequiredAttribList = new ArrayList<Integer>();
	List<Integer> lstCondAttribList = new ArrayList<Integer>();
	List<Integer> lstGroupByAttribList = new ArrayList<Integer>();
	StringBuilder strMapValueBuilder = new StringBuilder("");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		// Get the source index; (vins.data = 2, achats.data = 1)
		// Added as configuration in driver
		FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
		intSrcIndex = Integer.parseInt(context.getConfiguration().get(
				fsFileSplit.getPath().getName()));
		
		// Initialize the list of fields to emit as output based on
		// intSrcIndex (1=achats.data, 2=vins.data)
		if (intSrcIndex == 1) // achats
		{
			lstRequiredAttribList.add(0); 
			lstRequiredAttribList.add(1); 
			lstRequiredAttribList.add(2); 
			lstRequiredAttribList.add(3); 
			lstRequiredAttribList.add(4); 
		} else // vins
		{
			lstRequiredAttribList.add(3); 
		}
	}
	
	//Constitute the map value.  
	private String buildMapValue(String arrEntityAttributesList[]) {
		
		strMapValueBuilder.setLength(0);// Initialize
		//If the array has 4 entity, we can decide that it is from the source of vins(x).
		//Else if the array has 5 entity, we can decide that it is from the source of achats(y).
		
		strMapValueBuilder.append(intSrcIndex).append(",");
		for(int i = 0; i < arrEntityAttributesList.length; i++)
			if(lstRequiredAttribList.contains(i))
				strMapValueBuilder.append(arrEntityAttributesList[i]).append(",");
			
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
			
			//Set the output value.
			outputValue.set(buildMapValue(arrEntityAttributes));
			//Write the key,value into output
			if(outputValue.getLength() != 0){
				outputKey.set(arrEntityAttributes[0]); 
				context.write(outputKey,outputValue);
			}
		}
	}
}
