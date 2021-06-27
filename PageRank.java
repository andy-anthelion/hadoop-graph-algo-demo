package as3;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class PageRank extends BasicComputation
  <LongWritable,DoubleWritable,FloatWritable,DoubleWritable>
{
  private static int MAX_STEPS = 50;

  @Override
  public void compute(
  Vertex<LongWritable,DoubleWritable,FloatWritable> vertex,
  Iterable<DoubleWritable> messages) throws IOException 
  {
    if(getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(0.0));
    } else if(getSuperstep() == 1) {
      vertex.setValue(new DoubleWritable(1.0 / getTotalNumVertices()));
    } else {
      double total = 0.0;
      for(DoubleWritable msg : messages){
        total += msg.get();
      }
      vertex.setValue(
        new DoubleWritable(
          0.15 / getTotalNumVertices() + 0.85 * total 
        )
      );
    }

    if(getSuperstep() < MAX_STEPS) {
      long ec = vertex.getNumEdges();
      sendMessageToAllEdges(
        vertex, 
        new DoubleWritable(vertex.getValue().get() / ec)
      );
    } else {
      vertex.voteToHalt();
    }
  }
};
