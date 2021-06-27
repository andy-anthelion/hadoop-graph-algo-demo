package as3;

import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;

import org.apache.giraph.master.DefaultMasterCompute;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;

import org.apache.giraph.io.VertexReader;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

import java.util.Random;

public class GraphColor extends BasicComputation
<LongWritable,DoubleWritable,FloatWritable,GCMessage>
{
  private static final String PHASE  = "graphcolor.phase";
  private static final String COLOR  = "graphcolor.color";
  private static final String UCOUNT = "graphcolor.ucount";

  public static class GraphColorMaster extends DefaultMasterCompute
  {
    public static int color = 0;
    public static int phase = 0;

    @Override
    public void initialize() throws 
    InstantiationException, IllegalAccessException 
    {
      registerAggregator(COLOR,IntOverwriteAggregator.class);
      registerAggregator(PHASE,IntOverwriteAggregator.class);
      registerAggregator(UCOUNT,LongSumAggregator.class);
    }

    @Override
    public void compute() 
    {
      long ucount = 0L;

      //Initialize color and phase to 0
      if(getSuperstep() == 0){

        color = 0;
        phase = 0;

      }else{

        //Follow each phase as described in paper
        switch(phase){
          case 0: case 1: case 2: case 3: case 4: case 6:
            phase = (phase + 1) % 7;
          break;
          case 5:
            ucount = ((LongWritable)getAggregatedValue(UCOUNT)).get();
            phase = (ucount == 0L)? 6 : 2;
          break;
        }

        //Update phase value
        setAggregatedValue(PHASE,new IntWritable(phase));

        //If enterted color assignment phase,set graph color
        if(phase == 6){
          setAggregatedValue(COLOR,new IntWritable(color));
          color += 1;
        }
      }
    }    
  };

  private double gcPackValue(double mode, double degree)
  {
    return ((mode * 1e7) - degree);
  }

  private boolean gcIsValueColor(double val)
  {
    return (val >= 0.0);
  }

  private double gcGetMode(double val)
  {
    return Math.ceil(val / 1e7);
  }

  private double gcGetDegree(double val)
  {
    return Math.abs(val % 1e7);
  }

  @Override
  public void compute(
  Vertex<LongWritable,DoubleWritable,FloatWritable> vertex,
  Iterable<GCMessage> messages) throws IOException 
  {
    double degree = 0.0;
    double mode = 0.0;
    int color = 0;

    double value = vertex.getValue().get();    
    long id = vertex.getId().get();

    //Check if vertex is already coloured
    if(gcIsValueColor(value) && getSuperstep() > 0) {
      vertex.voteToHalt();
      return;
    }

    //Check if vertex is isolated
    if(vertex.getNumEdges() == 0){
      vertex.setValue(new DoubleWritable(0.0));
      vertex.voteToHalt();
      return;
    }

    int phase = ((IntWritable)getAggregatedValue(PHASE)).get();

    if(phase == 0){ //MIS Degree Initialization - 1

      //Set all uncolored vertex to Unknown(-1)
      vertex.setValue(new DoubleWritable(gcPackValue(-1.0,0.0)));
      sendMessageToAllEdges(vertex, new GCMessage(id,value));

    }else if(phase == 1){ //MIS Degree Initialization - 2

      //Get degree of vertex from previous step and store in value
      mode = gcGetMode(value);
      degree = 0.0;
      for(GCMessage msg : messages){
        degree += 1.0;
      }
      vertex.setValue(new DoubleWritable(gcPackValue(mode,degree)));

    }else if(phase == 2){ //Selection

      //Unknown Nodes become either TentativelyinS (-2) or Unknown(-1)
      mode = gcGetMode(value);
      degree = gcGetDegree(value);
      Random random = new Random();

      //If InS or NotInS then ignore
      if(mode != -1.0){
        return;
      }

      if(random.nextDouble() < (0.5 / degree)){
        mode = -2.0; //TentaivelyInS
        value = gcPackValue(mode,degree);
        vertex.setValue(new DoubleWritable(value));
        sendMessageToAllEdges(vertex, new GCMessage(id,value));
      } else {
        mode = -1.0; //Unknown
        vertex.setValue(new DoubleWritable(gcPackValue(mode,degree)));
      }

    }else if(phase == 3){ // Conflict-Resolution

      //Find best candidate to move to InS and 
      //set remaining to Unknown
      mode = gcGetMode(value);
      degree = gcGetDegree(value);
   
      if(mode != -2.0){ // Check if TentativelyInS
        return;
      }

      boolean min = true;
      for(GCMessage msg : messages){
        min = (id > msg.id)? false : min;
      }

      if(min == true){
        mode = -3.0; // In S
        value = gcPackValue(mode,degree);
        vertex.setValue(new DoubleWritable(value));
        sendMessageToAllEdges(vertex, new GCMessage(id,value));                
      } else {
        mode = -1.0; // Unknown
        value = gcPackValue(mode,degree);
        vertex.setValue(new DoubleWritable(value));
      }

    }else if(phase == 4){ //NotInS Discovery and Degree 1 Adjusting

      mode = gcGetMode(value);
      degree = gcGetDegree(value);

      //Check if message has InS. Then move node to NotInS    
      for(GCMessage msg : messages){
        if(gcGetMode(msg.value) == -3.0){
          mode = -4.0; // NotInS
          value = gcPackValue(mode,degree);
          vertex.setValue(new DoubleWritable(value));
          sendMessageToAllEdges(vertex, new GCMessage(id,value));                
          return;
        }
      }

    }else if(phase == 5){ //Degree Adjusting 2

      mode = gcGetMode(value);
      degree = gcGetDegree(value);

      if(mode != -1.0){ //Check if in Unknown
        return;
      }

      aggregate(UCOUNT,new LongWritable(1L));

      for(GCMessage msg : messages){
        degree -= 1.0;
      }
      
    }else if(phase == 6) { // Color vertex

      mode = gcGetMode(value);
      degree = gcGetDegree(value);
      color = ((IntWritable)getAggregatedValue(COLOR)).get();

      if(mode == -3.0){
        value = (double)color;
      }

      if(mode == -4.0){
        value = gcPackValue(-1.0,degree);
      }

      vertex.setValue(new DoubleWritable(value));        

    } else {
      
    }
  }
};
