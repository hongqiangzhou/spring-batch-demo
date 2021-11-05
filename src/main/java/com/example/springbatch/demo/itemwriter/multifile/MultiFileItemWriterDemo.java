package com.example.springbatch.demo.itemwriter.multifile;

import com.example.springbatch.demo.BaseConfig;
import com.example.springbatch.entity.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.tomcat.util.buf.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//@Configuration
public class MultiFileItemWriterDemo extends BaseConfig {

    @Autowired
    private DataSource dataSource;

    @Bean
    public Job multiFileItemWriterDemoJob() throws Exception {
        return jobBuilderFactory.get("multiFileItemWriterDemoJob")
                .start(multiFileItemWriterDemoStep())
                .build();
    }

    @Bean
    public Step multiFileItemWriterDemoStep() throws Exception {
        return stepBuilderFactory.get("multiFileItemWriterDemoStep")
                .<User, User>chunk(2)
                .reader(dbJdbcReader())
                //.writer(multiFileItemWriter())
                .writer(classfierMultiFileItemWriter())
                .stream(jsonFileWriter())
                .stream(xmlFileWriter())
                .build();
    }

    @Bean
    @StepScope
    public ClassifierCompositeItemWriter<User> classfierMultiFileItemWriter() {
        ClassifierCompositeItemWriter<User> writer = new ClassifierCompositeItemWriter<>();
        writer.setClassifier(new Classifier<User, ItemWriter<? super User>>() {
            @SneakyThrows
            @Override
            public ItemWriter<? super User> classify(User user) {
                // Classify by user ID
                return user.getId() % 2 == 0? jsonFileWriter() : xmlFileWriter();
            }
        });

        return writer;
    }

    //@Bean
    //@StepScope
    public CompositeItemWriter<User> multiFileItemWriter() throws Exception {
        CompositeItemWriter<User> writer = new CompositeItemWriter<>();
        writer.setDelegates(Arrays.asList(jsonFileWriter(), xmlFileWriter()));
        writer.afterPropertiesSet();
        return writer;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<User> jsonFileWriter() throws Exception {
        FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
        Character sep = System.getProperty("file.separator").charAt(0);
        String path = StringUtils.join(Arrays.asList(System.getProperty("user.dir"), "data", "user-out.csv"), sep);
        writer.setResource(new FileSystemResource(path)); // File "customer-out.csv" will be created automatically in the "spring-batch-demo/data" folder.

        writer.setLineAggregator(new LineAggregator<User>() {
            @Override
            public String aggregate(User user) {
                ObjectMapper mapper = new ObjectMapper();
                String str = null;
                try {
                    str = mapper.writeValueAsString(user);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                return str;
            }
        });
        writer.afterPropertiesSet();
        return writer;
    }

    @Bean
    @StepScope
    public StaxEventItemWriter<User> xmlFileWriter() throws Exception {
        StaxEventItemWriter<User> writer = new StaxEventItemWriter<>();

        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setAliases(new HashMap<>() {{
            put("user", User.class);
        }});

        writer.setRootTagName("users");
        writer.setMarshaller(marshaller);

        Character sep = System.getProperty("file.separator").charAt(0);
        String path = StringUtils.join(Arrays.asList(System.getProperty("user.dir"), "data", "user-out.xml"), sep);
        writer.setResource(new FileSystemResource(path));
        writer.afterPropertiesSet();

        return writer;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<? extends User> dbJdbcReader() {
        JdbcPagingItemReader<User> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setFetchSize(2);
        reader.setRowMapper(new RowMapper<User>() {
            @Override
            public User mapRow(ResultSet rs, int rowNum) throws SQLException {
                User user = new User();
                user.setId(rs.getInt(1));
                user.setUsername(rs.getString(2));
                user.setPassword(rs.getString(3));
                user.setAge(rs.getInt(4));
                return user;
            }
        });

        // query
        H2PagingQueryProvider provider = new H2PagingQueryProvider();
        provider.setSelectClause("id, username, password, age");
        provider.setFromClause("from user");

        // sorting
        Map<String, Order> sort = new HashMap<>();
        sort.put("id", Order.ASCENDING);
        provider.setSortKeys(sort);

        reader.setQueryProvider(provider);
        return reader;
    }
}
