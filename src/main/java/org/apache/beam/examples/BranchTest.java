package org.apache.beam.examples;

import java.util.ArrayList;
import java.util.Random;
import java.util.SplittableRandom;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

public class BranchTest {

    static class CreateSystem extends DoFn<Long, Document> {
        @ProcessElement
        public void processElement(ProcessContext c){
            Document a = new Document();
            a.append("name", new Random().nextLong());
            c.output(a);
        }
    }

    static class CreateStar extends DoFn<Document, Document> {
        @ProcessElement
        public void processElement(ProcessContext c){
            int totalStar = new SplittableRandom().nextInt(1, 3);
            for (int i = 0; i < totalStar; i++) {
                Document a = new Document();
                a.append("name", new Random().nextLong());
                c.output(a);
            }
        }
    }

    static class CreatePlanet extends DoFn<Document, Document> {
        @ProcessElement
        public void processElement(ProcessContext c){
            int totalStar = new SplittableRandom().nextInt(3, 15);
            for (int i = 0; i < totalStar; i++) {
                Document a = new Document();
                a.append("name", new Random().nextLong());
                c.output(a);
            }
        }
    }

    static class CreateAsteroid extends DoFn<Document, Document> {
        @ProcessElement
        public void processElement(ProcessContext c){
            int totalStar = new SplittableRandom().nextInt(100, 1000000);
            for (int i = 0; i < totalStar; i++) {
                Document a = new Document();
                a.append("name", new Random().nextLong());
                c.output(a);
            }
        }
    }

    public interface BranchTestOptions extends PipelineOptions {
      
    }

    public static void main(String[] args) {
        BranchTestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BranchTestOptions.class);

        ArrayList<Long> seedList = new ArrayList<Long>();
        for (long i = 0; i < 100; i++) {
            seedList.add(i);
        }
        
        Pipeline p = Pipeline.create(options);
        PCollection<Long> seeds =  p.apply(Create.of(seedList));
        
        PCollection<Document> systemIds =  seeds.apply("Create System", ParDo.of(new CreateSystem()));

        PCollection<Document> starIds =  systemIds.apply("Create Star", ParDo.of(new CreateStar()));

        PCollection<Document> planetId =  starIds.apply("Create Planet", ParDo.of(new CreatePlanet()));
        PCollection<Document> moonId =  planetId.apply("Create Moon", ParDo.of(new CreatePlanet()));

        PCollection<Document> asteroidId =  starIds.apply("Create Asteroid", ParDo.of(new CreateAsteroid()));

        String mongoURI = "mongodb://localhost:27017";

        systemIds.apply("Write System", MongoDbIO.write()
            .withUri(mongoURI)
            .withDatabase("my-database")
            .withCollection("my-collection")
        );

        starIds.apply("Write Star", MongoDbIO.write()
            .withUri(mongoURI)
            .withDatabase("my-database")
            .withCollection("stars")
        );

        planetId.apply("Write Planet", MongoDbIO.write()
            .withUri(mongoURI)
            .withDatabase("my-database")
            .withCollection("planets")
        );

        moonId.apply("Write Moon", MongoDbIO.write()
            .withUri(mongoURI)
            .withDatabase("my-database")
            .withCollection("moons")
        );

        asteroidId.apply("Write AsteroidId", MongoDbIO.write()
            .withUri(mongoURI)
            .withDatabase("my-database")
            .withCollection("asteroids")
        );  

        p.run().waitUntilFinish();
    }
}
