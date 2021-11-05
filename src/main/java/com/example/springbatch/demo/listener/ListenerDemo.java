package com.example.springbatch.demo.listener;

import com.example.springbatch.demo.BaseConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;

//@Configuration
public class ListenerDemo extends BaseConfig {

    @Autowired
    @Qualifier(value = "myJobListener")
    private JobExecutionListener myJobListener;

    @Autowired
    @Qualifier(value = "myChunkListener")
    private MyChunkListener myChunkListener;

    @Bean
    public Job listenerJob(){
        return jobBuilderFactory.get("listenerJob")
                .start(step1())
                .listener(myJobListener)
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(2)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(reader())
                .writer(writer())
                .build();
    }

    @Bean
    public ItemReader<String> reader() {
        return new ListItemReader<String>(Arrays.asList("java", "spring", "mybatis"));
    }

    @Bean
    public ItemWriter<String> writer() {
        return new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> list) throws Exception {
                list.forEach(System.out::println);
            }
        };
    }
}
