package com.example.springbatch.demo.itemreader.restart;

import com.example.springbatch.demo.BaseConfig;
import com.example.springbatch.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

import java.util.List;

//@Configuration
public class RestartDemo extends BaseConfig {

    @Autowired
    @Qualifier("restartReader")
    private RestartReader restartReader;
    
    @Bean
    public Job restartDemoJob() {
        return jobBuilderFactory.get("restartDemoJob")
                .start(restartDemoStep())
                .build();
    }

    @Bean
    public Step restartDemoStep() {
        return stepBuilderFactory.get("restartDemoStep")
                .<Customer, Customer>chunk(3)
                .reader(restartReader)
                .writer(restartWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<Customer> restartWriter() {
        ItemWriter<Customer> writer = new ItemWriter<>() {
            @Override
            public void write(List<? extends Customer> list) throws Exception {
                list.forEach(System.out::println);
            }
        };
        return writer;
    }

}
