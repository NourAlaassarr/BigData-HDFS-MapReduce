
//Importing libraries 
import java.io.IOException; 




import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;



public class DifferenceMapper extends Mapper<LongWritable, Text, Text, Text>  {
	private Text table= new Text();
	private Text num= new Text();
	public void map(LongWritable key,Text record, Context context) throws IOException, InterruptedException {
		String[] fields = record.toString().split(",");
		String tablename = fields[0];
		String value = "";
		if(tablename.equals("T1")){
		value = fields[1];
	}	else{value = fields[2];
	}	
		table.set(tablename);
		num.set(value);
		
		context.write(table,num);
	}
}
