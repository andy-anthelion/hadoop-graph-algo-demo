package as3;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;

import org.apache.giraph.graph.BasicComputation;


import org.apache.giraph.io.VertexReader;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class WeakConnect extends BasicComputation
  <LongWritable,DoubleWritable,FloatWritable,DoubleWritable>
{
  @Override
  public void compute(
  Vertex<LongWritable,DoubleWritable,FloatWritable> vertex,
  Iterable<DoubleWritable> messages) throws IOException 
  {
    if(getSuperstep() == 0) {
      long id = vertex.getId().get();
      //Initialize vertex value with id
      vertex.setValue(new DoubleWritable((double)id));

      //Send message to all neighbours
      sendMessageToAllEdges(vertex, vertex.getValue());
    } else {

      double min = vertex.getValue().get();

      for(DoubleWritable msg : messages){
        min = (min > msg.get()) ? msg.get() : min;
      }

      if(min == vertex.getValue().get()){
        vertex.voteToHalt();
      } else {

        //Update vertex value
        vertex.setValue(new DoubleWritable(min));

        //Send message to all neighbours
        sendMessageToAllEdges(vertex, vertex.getValue());
      }      
    }

    vertex.voteToHalt();
  }
};
