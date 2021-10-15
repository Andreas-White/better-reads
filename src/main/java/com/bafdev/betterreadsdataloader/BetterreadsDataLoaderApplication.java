package com.bafdev.betterreadsdataloader;

import com.bafdev.betterreadsdataloader.author.Author;
import com.bafdev.betterreadsdataloader.author.AuthorRepository;
import com.bafdev.betterreadsdataloader.book.Book;
import com.bafdev.betterreadsdataloader.book.BookRepository;
import com.bafdev.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    AuthorRepository authorRepository;
    BookRepository bookRepository;

    @Autowired
    public void setAuthorRepository(AuthorRepository authorRepository) {
        this.authorRepository = authorRepository;
    }

    @Autowired
    public void setBookRepository(BookRepository bookRepository) {
        this.bookRepository = bookRepository;
    }

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    public void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try {
            Stream<String> lines = Files.lines(path);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initWorks() {
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        Path path = Paths.get(worksDumpLocation);
        try {
            Stream<String> lines = Files.lines(path);
            lines.limit(100).forEach(line -> {
                try {
                    // Read and parse the line
                    String jsonString = line.substring(line.indexOf("{"));
                    JSONObject jsonObject = new JSONObject(jsonString);

                    // Construct Book object
                    Book book = new Book();
                    //Book id
                    book.setId(jsonObject.optString("key").replace("/works/", ""));
                    // Book name
                    book.setName(jsonObject.optString("title"));
                    // Book description
                    JSONObject description = jsonObject.optJSONObject("description");
                    if (description != null)
                        book.setDescription(description.optString("value"));
                    // Book cover ids
                    JSONArray coversArray = jsonObject.optJSONArray("covers");
                    if (coversArray != null) {
                        List<String> coverIds = new ArrayList<>();
                        for (int i = 0; i < coversArray.length(); i++) {
                            coverIds.add(coversArray.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }
                    // Book author ids
                    JSONArray authorsArray = jsonObject.optJSONArray("authors");
                    if (authorsArray != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < authorsArray.length(); i++) {
                            String authorId = authorsArray.getJSONObject(i).getJSONObject("author")
                                    .getString("key")
                                    .replace("/authors/","");
                            authorIds.add(authorId);
                        }
                        book.setAuthorIds(authorIds);
                        // Book author names
                        List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if (!optionalAuthor.isPresent()) return "Unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }
                    // Book published date
                    JSONObject published = jsonObject.optJSONObject("created");
                    if (published != null) {
                        String dateStr = published.getString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, format));
                    }


                    // Persist using Repository
                    System.out.println("Saving book " + book.getName() + "...");
                    bookRepository.save(book);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @PostConstruct
    public void start() {
        //initAuthors();
        initWorks();
        System.out.println(authorDumpLocation);
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
