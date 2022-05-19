import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.lang.StringUtils;
import java.util.*;
import java.util.Date;

public class TimeMapper extends Mapper<Object, Text, Text, IntWritable> 
{ 
	private Text data = new Text();
    	private final IntWritable one = new IntWritable(1);
    
   	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{	
			String tweets = value.toString();
			if(StringUtils.ordinalIndexOf(tweets,";",3)>-1)
			{
				int startIndex = StringUtils.ordinalIndexOf(tweets,";",0) + 1;
				int finishIndex = StringUtils.ordinalIndexOf(tweets, ";", 1);
				String epochtime = tweets.substring(startIndex,finishIndex);
				Date tweetdate = new Date(Long.parseLong(epochtime));
				String[] fullDate = tweetdate.toString().split(" ");
				String date = fullDate[2]+ " " + fullDate[1] + " " + fullDate[5];
				data.set(date);
				context.write(data, one);

			}
	}
}    

