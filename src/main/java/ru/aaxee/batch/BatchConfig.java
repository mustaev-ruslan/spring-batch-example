package ru.aaxee.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import ru.aaxee.batch.domain.Book;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@Log
@RequiredArgsConstructor
@EnableBatchProcessing
public class BatchConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job exampleJob(
            Step csvToH2Step
            ,Step migrateStep
            ,Step mongoToCsvStep
    ) {
        return jobBuilderFactory.get("exampleJob")
                .incrementer(new RunIdIncrementer())
                .start(csvToH2Step)
                .next(migrateStep)
                .next(mongoToCsvStep)
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        log.info("Начало job");
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        log.info("Конец job");
                    }
                })
                .build();
    }

    @Bean
    public Step csvToH2Step(FlatFileItemReader<Book> csvReader, ItemWriter h2Writer) {
        return stepBuilderFactory.get("csvToH2Step")
                .chunk(5)
                .reader(csvReader)
                .writer(h2Writer)
                .build();
    }

    @Bean
    public Step migrateStep(ItemReader h2Reader, ItemWriter mongoWriter) {
        //noinspection unchecked
        return stepBuilderFactory.get("migrateStep")
                .chunk(5)
                .reader(h2Reader)
                .writer(mongoWriter)
                .listener(new ItemReadListener() {
                    @Override
                    public void beforeRead() {

                    }

                    @Override
                    public void afterRead(Object item) {
                        log.info("READ from H2: " + item.toString());
                    }

                    @Override
                    public void onReadError(Exception ex) {

                    }
                })
                .build();
    }

    @Bean
    public Step mongoToCsvStep(ItemReader mongoReader, ItemWriter csvWriter) {
        return stepBuilderFactory.get("mongoToCsvStep")
                .chunk(5)
                .reader(mongoReader)
                .writer(csvWriter)
                .build();
    }

    @Bean
    public FlatFileItemReader<Book> csvReader() {
        return new FlatFileItemReaderBuilder<Book>()
                .name("bookItemReader")
                .resource(new ClassPathResource("books.csv"))
                .delimited()
                .names(new String[]{"id", "name"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Book>() {{
                    setTargetType(Book.class);
                }})
                .build();
    }

    @Bean
    public ItemWriter<Book> csvWriter() {
        return new FlatFileItemWriterBuilder<Book>()
                .name("bookItemWriter")
                .resource(new FileSystemResource("books2.csv"))
                .lineAggregator(new DelimitedLineAggregator<>())
                .build();
    }

    private static final String QUERY_INSERT_BOOK = "INSERT " +
            "INTO books(id, name) " +
            "VALUES (:id, :name)";

    @Bean
    ItemWriter<Book> h2Writer(DataSource dataSource,
                              NamedParameterJdbcTemplate jdbcTemplate) {
        JdbcBatchItemWriter<Book> databaseItemWriter = new JdbcBatchItemWriter<>();
        databaseItemWriter.setDataSource(dataSource);
        databaseItemWriter.setJdbcTemplate(jdbcTemplate);
        databaseItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        databaseItemWriter.setSql(QUERY_INSERT_BOOK);
        return databaseItemWriter;
    }

    private static final String QUERY_FIND_BOOKS =
            "SELECT id, name FROM books";

    @Bean
    ItemReader<Book> h2Reader(DataSource dataSource) {
        JdbcCursorItemReader<Book> databaseReader = new JdbcCursorItemReader<>();

        databaseReader.setDataSource(dataSource);
        databaseReader.setSql(QUERY_FIND_BOOKS);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Book.class));

        return databaseReader;
    }

    @Bean
    ItemWriter<Book> mongoWriter(MongoOperations mongoTemplate) {
        MongoItemWriter<Book> mongoWriter = new MongoItemWriter<>();
        mongoWriter.setTemplate(mongoTemplate);
        return mongoWriter;
    }

    @Bean
    public ItemReader<Book> mongoReader(MongoOperations mongoTemplate) {
        MongoItemReader<Book> mongoReader = new MongoItemReader<>();
        mongoReader.setTemplate(mongoTemplate);
        mongoReader.setTargetType(Book.class);
        mongoReader.setQuery("{}");
        Map<String, Sort.Direction> sorts = new HashMap<>(1);
        sorts.put("status", Sort.Direction.ASC);
        mongoReader.setSort(sorts);
        return mongoReader;
    }


}
