package Top10FlightDistance;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static final int K = 10;
	private TreeMap<Integer, Text> tMap;

	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		this.tMap = new TreeMap<Integer, Text>();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");

		try {

			String score = tokens[39].replace("\"","").trim();
			tMap.put(Integer.parseInt(score), new Text(value));

		} catch (Exception ex) {

		}

		if (tMap.size() > K) {
			tMap.remove(tMap.firstKey());
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		for (Text t : tMap.values()) {
			context.write(NullWritable.get(), t);
		}
		
	}

}
