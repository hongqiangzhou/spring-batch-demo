package com.example.springbatch.demo.itemwriter.basic;

import com.example.springbatch.demo.BaseConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//@Configuration
public class ItemWriterDemo extends BaseConfig {

    @Autowired
    @Qualifier(value = "myWriter")
    private ItemWriter<String> myWriter;

    @Bean
    public Job itemWriterDemoJob() {
        return jobBuilderFactory.get("itemWriterDemoJob")
                .start(itemReaderDemoStep())
                .build();
    }

    @Bean
    public Step itemReaderDemoStep() {
        return stepBuilderFactory.get("itemReaderDemoStep")
                .<String, String>chunk(5)
                .reader(myReader())
                .writer(myWriter)
                .build();
    }

    @Bean
    public ItemReader<String> myReader() {
        List<String> items = IntStream.rangeClosed(1, 50).boxed()
                .map(e -> "java " + e)
                .collect(Collectors.toList());
        return new ListItemReader<String>(items);
    }
}
