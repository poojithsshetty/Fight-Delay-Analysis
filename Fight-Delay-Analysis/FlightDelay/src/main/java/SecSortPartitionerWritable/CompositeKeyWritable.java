package SecSortPartitionerWritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

	private String state;
	private String airportId;

	public CompositeKeyWritable() {
		super();
	}

	public CompositeKeyWritable(String state, String id) {
		super();
		this.state = state;
		this.airportId = id;
	}

	public void write(DataOutput out) throws IOException {

		out.writeUTF(state);
		out.writeUTF(airportId);
	}

	public void readFields(DataInput in) throws IOException {

		state = in.readUTF();
		airportId = in.readUTF();
	}

	public int compareTo(CompositeKeyWritable o) {
		int result = this.state.compareTo(o.state);
		if (result == 0) {
			return this.airportId.compareTo(o.airportId);
		}
		return result;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getAirportId() {
		return airportId;
	}

	public void setAirportId(String airportId) {
		this.airportId = airportId;
	}

	@Override
	public String toString() {

		return state + "," + airportId;
	}

}
