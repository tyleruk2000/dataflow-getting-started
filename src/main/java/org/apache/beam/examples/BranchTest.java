package org.apache.beam.examples;

import java.util.ArrayList;
import java.util.Random;
import java.util.SplittableRandom;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;

public class BranchTest {
    public interface BranchTestOptions extends PipelineOptions {
      
    }

    public static void main(String[] args) {
        BranchTestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BranchTestOptions.class);

        ArrayList<Long> seeds = new ArrayList<Long>();
        seeds.add(0l);

        Pipeline p = Pipeline.create(options);
        PCollection<Long> a =  p.apply(Create.of(seeds));

        p.run().waitUntilFinish();
    }
}
