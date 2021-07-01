package org.apache.beam.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class HelloWorld{

    public interface HelloWorldOptions extends PipelineOptions {
      
    }
      
    public static void main(String[] args) {
        HelloWorldOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(HelloWorldOptions.class);

        ArrayList<Integer> test = new ArrayList<Integer>();
        for(int i=0; i<10;i++){
          test.add(i);
        }

        Pipeline p = Pipeline.create(options);
        
        p.apply(Create.of(test))
        
        .apply("Do Work", ParDo
          .of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              long startTime = System.currentTimeMillis();
              for(int i=1;i<20;i++){
                int a = 66*i;
                int b = a*i;
                Double rnd = new SplittableRandom().nextDouble();
                Double d = a*b*rnd;
                // c.output(d); Output the work data if needed
              }
              long endTime = System.currentTimeMillis();
              double duration = (double)(endTime - startTime)/1000;
              System.out.println("Took " + duration + " Seconds");
              c.output(c.element());
            }
          }))
        
          .apply(ToString.elements())
        
          .apply("WriteCounts", TextIO.write().to("Test"));

        p.run().waitUntilFinish();
    }
}