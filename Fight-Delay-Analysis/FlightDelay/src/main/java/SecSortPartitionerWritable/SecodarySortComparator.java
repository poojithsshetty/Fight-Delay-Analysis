package SecSortPartitionerWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecodarySortComparator extends WritableComparator {

	protected SecodarySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		CompositeKeyWritable ckw1 = (CompositeKeyWritable) a;
		CompositeKeyWritable ckw2 = (CompositeKeyWritable) b;

		int result = ckw1.getState().compareTo(ckw2.getState());

		if (result == 0) {
			return ckw1.getAirportId().compareTo(ckw2.getAirportId());
		}

		return result;
	}

	
	
}
