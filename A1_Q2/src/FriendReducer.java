import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendReducer extends Reducer<Text, Text, Text, Text>{

    private Set<String> friendsOfFriends;

    @Override
    protected void setup(Context context) {
        friendsOfFriends = new HashSet<>();
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	 String friendOfP1 = key.toString(); // Friend of P1 (key)
        // Collect all potential friends of friends (values)
        for (Text potentialFriendOfFriend : values) {
          String friend = potentialFriendOfFriend.toString();
          // Exclude P1 from the results
          if (!friend.equals("P1")) {
            friendsOfFriends.add(friend);
          }
        }
    	 
    	 
    	 
//    	 for (Text potentialFriendOfFriend : values) {
//    	      String friend = potentialFriendOfFriend.toString();
//    	      friendsOfFriends.add(friend); // Add all encountered friends
//    	    }
//    	 Set<String> friendsOfFriends = new HashSet<>();
//    	    for (String friend : friendsOfFriends) {
//    	      if (!friend.equals("P1") && !friendOfP1.equals(friend)) {
//    	        friendsOfFriends.add(friend);
//    	      }
//    	    }
    	 if (!friendsOfFriends.isEmpty()) {
            context.write(new Text(friendOfP1), new Text(","+friendsOfFriends));
          }


        // Reset friendsOfFriends for the next key
        friendsOfFriends.clear();
    }
}


