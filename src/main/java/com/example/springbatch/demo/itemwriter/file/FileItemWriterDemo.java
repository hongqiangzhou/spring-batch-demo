package com.example.springbatch.demo.itemwriter.file;

import com.example.springbatch.demo.BaseConfig;
import com.example.springbatch.entity.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tomcat.util.buf.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//@Configuration
public class FileItemWriterDemo extends BaseConfig {

    @Autowired
    private DataSource dataSource;

    @Bean
    public Job fileItemWriterDemoJob() throws Exception {
        return jobBuilderFactory.get("fileItemWriterDemoJob")
                .start(fileIteWriterDemoStep())
                .build();
    }

    @Bean
    public Step fileIteWriterDemoStep() throws Exception {
        return stepBuilderFactory.get("fileIteWriterDemoStep")
                .<User, User>chunk(2)
                .reader(dbJdbcReader())
                .writer(fileItemWriter())
                .build();
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

    @Bean
    @StepScope
    public FlatFileItemWriter<? super User> fileItemWriter() throws Exception {
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


}
