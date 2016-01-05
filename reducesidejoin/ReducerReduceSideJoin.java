package reducesidejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerReduceSideJoin extends Reducer<Text, Text, NullWritable, Text>{
	private Text outputValue = new Text("");
	private NullWritable outputKey = NullWritable.get();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		StringBuilder strBuilder = new StringBuilder("");
		List<String> achatslist = new ArrayList<String>();
		List<String> vinslist = new ArrayList<String>();
		
		for (Text val : values) { 
			String arrEntityAttributes[] = val.toString().split(",");
			if(arrEntityAttributes[0].equals("2"))
			{
				if(arrEntityAttributes.length<2)
					System.out.println("No second column for this vins record.");
				else
					vinslist.add(arrEntityAttributes[1]);
			}
			else if(arrEntityAttributes[0].equals("1"))
				achatslist.add(val.toString().replaceFirst("1,", ""));
		}
		if(achatslist.size()>0 && vinslist.size()>0){
			for(String xstr:achatslist){
				for(String ystr:vinslist){
					String str = xstr + "," + ystr;
					outputValue.set(str);
					context.write(outputKey, outputValue);
				}
			}
		} 
	}
}
