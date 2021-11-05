package com.example.springbatch.demo.itemreader.multifile;

import com.example.springbatch.demo.BaseConfig;
import com.example.springbatch.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.validation.BindException;

import java.util.List;

//@Configuration
public class MultiFileItemReaderDemo extends BaseConfig {

    @Value("classpath:/file*.txt")
    private Resource[] fileResources;

    @Bean
    public Job multiFileItemReaderDemoJob() {
        return jobBuilderFactory.get("multiFileItemReaderDemoJob")
                .start(multiFileIteReaderDemoStep())
                .build();
    }

    @Bean
    public Step multiFileIteReaderDemoStep() {
        return stepBuilderFactory.get("multiFileIteReaderDemoStep")
                .<Customer, Customer>chunk(2)
                .reader(multiFileReader())
                .writer(multiFileWriter())
                .build();
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<? extends Customer> multiFileReader() {
        MultiResourceItemReader<Customer> reader = new MultiResourceItemReader<>();
        reader.setDelegate(flatFileItemReader());
        reader.setResources(fileResources);
        return reader;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<? extends Customer> flatFileItemReader() {
        FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();

        DefaultLineMapper<Customer> mapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(new String[]{"id", "firstName", "lastName", "birthday"});
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

    @Bean
    @StepScope
    public ItemWriter<? super Customer> multiFileWriter() {
        ItemWriter<Customer> writer = new ItemWriter<Customer>() {
            @Override
            public void write(List<? extends Customer> list) throws Exception {
                list.forEach(System.out::println);
            }
        };

        return writer;
    }

}
