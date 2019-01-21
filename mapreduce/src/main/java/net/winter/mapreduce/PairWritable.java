package net.winter.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable> {
    public IntWritable x;
    public IntWritable y;


    public int compareTo(PairWritable o) {
        if(x.get()==o.x.get()) {
            return y.get() - o.y.get();
        }
        return x.get() - o.x.get();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(x.get());
        dataOutput.writeInt(y.get());

    }

    public void readFields(DataInput dataInput) throws IOException {
        x = new IntWritable(dataInput.readInt());
        y = new IntWritable(dataInput.readInt());
    }

}
