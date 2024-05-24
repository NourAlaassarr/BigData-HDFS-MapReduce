import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DifferenceMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
//		Configuration conf = new Configuration();
		  if (args.length != 2) {
	            System.err.println("Usage: DifferenceMain <input path> <output path>");
	            System.exit(-1);
	        }
		  Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Find Table-Specific Attributes"); // Set a descriptive job name

        job.setJarByClass(DifferenceMain.class);
        
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(DifferenceMapper.class);
        job.setReducerClass(DifferenceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
