package com.example.springbatch.demo.itemwriter.xml;

import com.example.springbatch.demo.BaseConfig;
import com.example.springbatch.entity.User;
import org.apache.tomcat.util.buf.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
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
public class XmlItemWriterDemo extends BaseConfig {

    @Autowired
    private DataSource dataSource;

    @Bean
    public Job XmlItemWriterDemoJob() throws Exception {
        return jobBuilderFactory.get("XmlItemWriterDemoJob")
                .start(xmlItemWriterDemoStep())
                .build();
    }

    @Bean
    public Step xmlItemWriterDemoStep() throws Exception {
        return stepBuilderFactory.get("xmlItemWriterDemoStep")
                .<User, User>chunk(2)
                .reader(dbJdbcReader())
                .writer(xmlItemWriter())
                .build();
    }

    @Bean
    public StaxEventItemWriter<User> xmlItemWriter() throws Exception {
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
