package com.bafdev.betterreadsdataloader;

import com.bafdev.betterreadsdataloader.author.Author;
import com.bafdev.betterreadsdataloader.author.AuthorRepository;
import com.bafdev.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.nio.file.Path;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    @Autowired
    AuthorRepository authorRepository;

    @PostConstruct
    public void start() {

    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
