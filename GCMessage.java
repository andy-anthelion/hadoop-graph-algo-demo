package as3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


  public class GCMessage implements Writable
  {
    public long id;
    public double value;

    public GCMessage() {}
    public GCMessage(long id, double value)
    {
      this.id = id;
      this.value = value;
    }

    @Override
    public void readFields(DataInput input) throws IOException 
    {
      id = input.readLong();
      value = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException 
    {
      output.writeLong(id);
      output.writeDouble(value);
    }

    @Override
    public String toString() 
    {
      return id + ":" + value;
    }
  };

