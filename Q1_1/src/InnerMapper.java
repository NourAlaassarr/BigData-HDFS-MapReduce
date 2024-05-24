
//Importing libraries 
import java.io.IOException; 





import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class InnerMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text table= new Text();
	private Text num= new Text();
	private Text pk= new Text();

	public void map(LongWritable key,Text record, Context context) throws IOException, InterruptedException {
		String[] fields = record.toString().split(",");
		String tablename = fields[0];
		String value = "";
		String pkvalue = "";

		if(tablename.equals("T1")){
			value = fields[2];
			pkvalue = fields[1];
		}	
		else{
			pkvalue = fields[2];
			value = fields[1];
		}	
		table.set(tablename);
		num.set(value);
		pk.set(pkvalue);
		StringBuilder sb = new StringBuilder();
		sb.append(table);
		sb.append(",");
		sb.append(num);
		String TableValue = sb.toString();
		context.write(pk,new Text(TableValue));
	}
}
