package com.example.springbatch.demo.itemreader.basic;

import com.example.springbatch.demo.BaseConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.item.ItemReader;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;

//@Configuration
public class ItemReaderDemo extends BaseConfig {

    @Bean
    public Job itemReaderDemoJob() {
        return jobBuilderFactory.get("itemReaderDemoJob")
                .start(itemReaderDemoStep())
                .build();
    }

    @Bean
    public Step itemReaderDemoStep() {
        return stepBuilderFactory.get("itemReaderDemoStep")
                .<String, String>chunk(2)
                .reader(itemReaderDemoReader())
                .writer(list -> list.forEach(item -> System.out.println(item + "...")))
                .build();
    }

    @Bean
    public ItemReader<String> itemReaderDemoReader() {
        List<String> data = Arrays.asList("cat", "dog", "pig", "duck");
        return new MyReader(data);
    }
}
