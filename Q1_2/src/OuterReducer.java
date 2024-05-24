

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class OuterReducer extends Reducer<Text, Text, Text, Text>{
	 	private HashMap<String, String> t1Values = new HashMap<String, String>();
	    private HashMap<String, String> t2Values = new HashMap<String, String>();

	    @Override
	    protected void reduce(Text key, Iterable<Text> valuesIter, Context context) throws IOException, InterruptedException {
	        // Collect values for each table based on key (A1)
	        for (Text value : valuesIter) {
	            String[] parts = value.toString().split(",");
	            if (parts.length > 1) { // Assuming table name is the first element
	                String table = parts[0];
	                String data = parts[1];
	                if (table.equals("T1")) {
	                    t1Values.put(key.toString(), data);
	                } else if (table.equals("T2")) {
	                    t2Values.put(key.toString(), data);
	                }
	            }
	        }
	    }
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        super.cleanup(context);

	        // Perform full outer join logic
	        for (String a1 : t1Values.keySet()) {
	            String t1Data = t1Values.get(a1);
	            String t2Data = t2Values.get(a1);
	            if (t2Data == null) {
	            	context.write(new Text(a1), new Text(t1Data+",null")); // Assuming missing value from T2 has "Y" in the second position
	            }else{
	            // Emit join results for T1
	            context.write(new Text(a1), new Text(t1Data + "," +t2Data));
	            }
	            // Emit join results for T2 (if A1 doesn't exist in T2)
	        }
	        for (String a1 : t2Values.keySet()) {
	            if (!t1Values.containsKey(a1)) { // Check if A1 exists in T1
	                String t2Data = t2Values.get(a1);
	                context.write(new Text(a1), new Text("null," + t2Data)); // Assuming missing value from T1 has "Z" in the second position
	            }
	        }
	    }
}
