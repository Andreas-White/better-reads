package com.bafdev.betterreadsdataloader;

import com.bafdev.betterreadsdataloader.author.Author;
import com.bafdev.betterreadsdataloader.author.AuthorRepository;
import com.bafdev.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    @Autowired
    AuthorRepository authorRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try {

            Stream<String> lines = Files.lines(path);
            long start = System.currentTimeMillis();
            lines.limit(1000).forEach(line -> {
                try {
                    // Read and parse the line
                    String jsonString = line.substring(line.indexOf("{"));
                    JSONObject jsonObject = new JSONObject(jsonString);

                    // Construct Author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    // Persist using Repository
                    System.out.println("Saving author " + author.getName() + "...");
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
            long end = System.currentTimeMillis();
            System.out.println((end-start)/1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initWorks() {
        Path path = Paths.get(worksDumpLocation);
        try {
            Stream<String> lines = Files.lines(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void start() {
        initAuthors();
        //initWorks();
        System.out.println(authorDumpLocation);
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
