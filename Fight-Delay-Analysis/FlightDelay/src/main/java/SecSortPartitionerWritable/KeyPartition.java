package SecSortPartitionerWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition extends Partitioner<CompositeKeyWritable, NullWritable> {

	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value, int numPartitions) {
		
		return key.getState().hashCode()%numPartitions;
		
	}

}
