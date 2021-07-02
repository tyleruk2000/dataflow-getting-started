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

    static class CreateSystem extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c){
            c.output(new Random().nextLong());
        }
    }

    static class CreateStar extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c){
            int totalStar = new SplittableRandom().nextInt(1, 3);
            for (int i = 0; i < totalStar; i++) {
                c.output(new Random().nextLong());
            }
        }
    }

    static class CreatePlanet extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c){
            int totalStar = new SplittableRandom().nextInt(3, 15);
            for (int i = 0; i < totalStar; i++) {
                c.output(new Random().nextLong());
            }
        }
    }

    static class CreateAsteroid extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c){
            int totalStar = new SplittableRandom().nextInt(100, 1000000);
            for (int i = 0; i < totalStar; i++) {
                c.output(new Random().nextLong());
            }
        }
    }

    public interface BranchTestOptions extends PipelineOptions {
      
    }

    public static void main(String[] args) {
        BranchTestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BranchTestOptions.class);

        ArrayList<Long> seedList = new ArrayList<Long>();
        seedList.add(0l);

        Pipeline p = Pipeline.create(options);
        PCollection<Long> seeds =  p.apply(Create.of(seedList));
        
        PCollection<Long> systemIds =  seeds.apply("Create System", ParDo.of(new CreateSystem()));

        PCollection<Long> startIds =  systemIds.apply("Create Star", ParDo.of(new CreateStar()));

        PCollection<Long> planetId =  startIds.apply("Create Planet", ParDo.of(new CreatePlanet()));
        PCollection<Long> moonId =  planetId.apply("Create Moon", ParDo.of(new CreatePlanet()));

        PCollection<Long> asteroidId =  startIds.apply("Create Asteroid", ParDo.of(new CreateAsteroid()));

        systemIds.apply(ToString.elements()).apply("WriteSystem", TextIO.write().to("System"));
        startIds.apply(ToString.elements()).apply("WriteStar", TextIO.write().to("Star"));
        planetId.apply(ToString.elements()).apply("WritePlanet", TextIO.write().to("Planet"));
        moonId.apply(ToString.elements()).apply("WriteMoon", TextIO.write().to("Moon"));
        asteroidId.apply(ToString.elements()).apply("WriteAsteroid", TextIO.write().to("Asteroid"));

        

        p.run().waitUntilFinish();
    }
}
