package org.apache.beam.examples;

import java.util.Arrays;
import java.util.List;

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
        Pipeline p = Pipeline.create(options);
        p.apply(Create.of(42,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,4442,22,33,21,33,22,33,44))
        .apply(ToString.elements())
        .apply("WriteCounts", TextIO.write().to("Test"));

        p.run().waitUntilFinish();
    }
}

//TODO --direct_num_workers