
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InnerMain {
	public static void main(String[] args) throws Exception {
		  //if (args.length != 2) {
        //System.err.println("Usage: DifferenceMain <input path> <output path>");
        //System.exit(-1);
    //}
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "InnerJoin"); // Set a descriptive job name
    job.setJarByClass(InnerMain.class);
    
    TextInputFormat.addInputPath(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(InnerMapper.class);
    job.setReducerClass(InnerReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}