package Top10FlightDistance;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private static final int K = 10;
	private TreeMap<Integer, Text> tMap;

	@Override
	protected void setup(Reducer<NullWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		this.tMap = new TreeMap<Integer, Text>();

	}

	@Override
	protected void reduce(NullWritable arg0, Iterable<Text> values,
                          Reducer<NullWritable, Text, NullWritable, Text>.Context arg2) throws IOException, InterruptedException {

		for (Text t : values) {
			String[] tokens = t.toString().split(",");
			tMap.put(Integer.parseInt(tokens[39].replace("\"","").trim()), new Text(t));

			if (tMap.size() > K) {
				tMap.remove(tMap.firstKey());
			}
		}
	}

	@Override
	protected void cleanup(Reducer<NullWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		for(Text t : tMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}

}
