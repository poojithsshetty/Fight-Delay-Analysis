package FlightCarrierDistanceMedian;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianStdDevTuple implements Writable {
    private  float median;
    private float stdDev;

    public MedianStdDevTuple(float median, float stdDev) {
        this.median = median;
        this.stdDev = stdDev;
    }

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public float getStdDev() {
        return stdDev;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.median);
        out.writeFloat(this.stdDev);
    }

    @Override
    public String toString() {

        return "median:"+ this.median +" Std_Div:"+this.stdDev;
    }

    public void readFields(DataInput in) throws IOException {
        this.median=in.readFloat();
        this.stdDev=in.readFloat();
    }


    public MedianStdDevTuple() {
    }
}
