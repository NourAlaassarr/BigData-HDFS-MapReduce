
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class DifferenceReducer  extends Reducer<Text, Text, Text, Text>{
	private HashSet<String> t1Values = new HashSet<String>();
    private HashSet<String> t2Values = new HashSet<String>(); // 
	    private Text result = new Text();
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	super.cleanup(context);
	        for (String value : t1Values) {
	            if (!t2Values.contains(value)) {
	                result.set(value);
	                context.write(new Text(","), result);
	            }
	        }
        }
    
	    
	public void reduce(Text tableName, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		 // Collect values for each table
        for (Text val : values) {
            String value = val.toString();
            if (tableName.toString().equals("T1")) {
                t1Values.add(value);
            } else if (tableName.toString().equals("T2")) {
                t2Values.add(value);
            }
        }
	}
}
