package com.example.springbatch.demo.errorhandle;

import com.example.springbatch.demo.BaseConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//@Configuration
public class SkipDemo extends BaseConfig {

    @Bean
    public Job skipDemoJob() {
        return jobBuilderFactory.get("skipDemoJob")
                .start(skipDemoStep())
                .build();
    }

    @Bean
    public Step skipDemoStep() {
        return stepBuilderFactory.get("skipDemoStep")
                .<String, String> chunk(10)
                .reader(reader())
                .processor(skipItemProcessor())
                .writer(skipItemWriter())
                .faultTolerant()
                .skip(CustomRetryException.class)
                .skipLimit(5)
                .build();
    }

    @Bean
    @StepScope
    public ListItemReader<String> reader() {
        List<String> items = IntStream.range(0, 60)
                .boxed()
                .map(String::valueOf)
                .collect(Collectors.toList());
        ListItemReader<String> reader = new ListItemReader<>(items);
        return reader;
    }

    @Bean
    @StepScope
    public ItemProcessor<? super String, String> skipItemProcessor() {
        ItemProcessor<String, String> processor = new ItemProcessor<String, String>() {
            private int attemptCount = 0;

            @Override
            public String process(String item) throws Exception {
                System.out.println("processing item " + item);
                if(item.equalsIgnoreCase("26")) {
                    attemptCount++;
                    if (attemptCount >= 3) {
                        System.out.println("Retried " + attemptCount + " times success.");
                        return String.valueOf(Integer.valueOf(item) * -1);
                    } else {
                        System.out.println("Proccessed " + attemptCount + " times fail.");
                        throw new CustomRetryException("Process failed. Attemt: " + attemptCount + " times.");
                    }
                } else {
                    return String.valueOf(Integer.valueOf(item) * -1);
                }
            }
        };

        return processor;
    }

    @Bean
    @StepScope
    public ItemWriter<? super String> skipItemWriter() {
        ItemWriter<String> writer = new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> list) throws Exception {
                list.forEach(System.out::println);
            }
        };

        return writer;
    }

    class CustomRetryException extends Exception {
        public CustomRetryException() {}

        public CustomRetryException(String msg) {
            super(msg);
        }
    }
}

