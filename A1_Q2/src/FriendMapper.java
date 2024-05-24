import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendMapper  extends Mapper<Object, Text, Text, Text> {
	private Map<String, List<String>> friends;
	  @Override
	    protected void setup(Context context) {
	        friends = new HashMap<>();
	    }

	    @Override
	    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        String[] pair = value.toString().split(",");
	        String person1 = pair[0].trim();
	        String person2 = pair[1].trim();
	        List<String> person1Friends = friends.get(person1);
	        if (person1Friends == null) {
	            person1Friends = new ArrayList<>();
	            friends.put(person1, person1Friends);
	        }
	        person1Friends.add(person2);

	        List<String> person2Friends = friends.get(person2);
	        if (person2Friends == null) {
	            person2Friends = new ArrayList<>();
	            friends.put(person2, person2Friends);
	        }
	        person2Friends.add(person1);

	        // Emit pairs for both directions
	        context.write(new Text(person1), new Text(person2));
	        
	    }

	
}
