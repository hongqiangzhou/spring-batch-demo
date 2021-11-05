package com.example.springbatch.demo.itemreader.file;

import com.example.springbatch.demo.BaseConfig;
import com.example.springbatch.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.validation.BindException;

//@Configuration
public class FileItemReaderDemo extends BaseConfig {

    @Autowired
    @Qualifier(value = "flatFileWriter")
    private FlatFileWriter flatFileWriter;

    @Bean
    public Job fileItemReaderJob() {
        return jobBuilderFactory.get("fileItemReaderJob")
                .start(fileItemReaderDemoStep())
                .build();
    }

    @Bean
    public Step fileItemReaderDemoStep() {
        return stepBuilderFactory.get("fileItemReaderDemoStep")
                .<Customer, Customer>chunk(2)
                .reader(flatFileReader())
                .writer(flatFileWriter)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<? extends Customer> flatFileReader() {
        FlatFileItemReader<Customer> reader = new FlatFileItemReader<Customer>();
        reader.setResource(new ClassPathResource("customer.txt"));
        reader.setLinesToSkip(1);

        // data ingest
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(new String[]{"id", "firstName", "lastName", "birthday"});

        // map to Customer object
        DefaultLineMapper<Customer> mapper = new DefaultLineMapper<>();
        mapper.setLineTokenizer(tokenizer);
        mapper.setFieldSetMapper(new FieldSetMapper<Customer>() {
            @Override
            public Customer mapFieldSet(FieldSet fieldSet) throws BindException {
                Customer customer = new Customer();
                customer.setId(fieldSet.readLong("id"));
                customer.setFirstName(fieldSet.readString("firstName"));
                customer.setLastName(fieldSet.readString("lastName"));
                customer.setBirthday(fieldSet.readString("birthday"));
                return customer;
            }
        });
        mapper.afterPropertiesSet();

        reader.setLineMapper(mapper);
        return reader;
    }


}
