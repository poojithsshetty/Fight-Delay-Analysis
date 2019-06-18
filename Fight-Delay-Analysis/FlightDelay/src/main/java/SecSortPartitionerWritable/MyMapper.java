package SecSortPartitionerWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] tokens = line.split(",");
		String state = null;
		String airportId = null;
	if(!value.toString().contains("YEAR")) {
	try {

		state = tokens[14].replace("\"", "").trim();
		airportId = tokens[10].replace("\"", "").trim();

	} catch (Exception e) {

	}

	if (state != null && airportId != null) {

		CompositeKeyWritable outKey = new CompositeKeyWritable(state, airportId);

		context.write(outKey, NullWritable.get());

	}
	}
	}

}
