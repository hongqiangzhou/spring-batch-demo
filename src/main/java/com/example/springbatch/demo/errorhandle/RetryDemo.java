package com.example.springbatch.demo.errorhandle;

import com.example.springbatch.demo.BaseConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//@Configuration
public class RetryDemo extends BaseConfig {

    @Bean
    public Job retryDemoJob() {
        return jobBuilderFactory.get("retryDemoJob")
                .start(retryDemoStep())
                .build();
    }

    @Bean
    public Step retryDemoStep() {
        return stepBuilderFactory.get("retryDemoStep")
                .<String, String>chunk(10)
                .reader(reader())
                .processor(retryItemProcessor())
                .writer(retryItemWriter())
                .faultTolerant()
                .retry(CustomRetryException.class)
                .retryLimit(5)
                .build();
    }

    @Bean
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
    public ItemWriter<? super String> retryItemWriter() {
        ItemWriter<String> writer = new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> list) throws Exception {
                list.forEach(System.out::println);
            }
        };
        return writer;
    }

    @Bean
    @StepScope
    public ItemProcessor<? super String, String> retryItemProcessor() {
        ItemProcessor<String, String> processor = new ItemProcessor<String, String>() {
            private int attemptCount = 0;
            @Override
            public String process(String item) throws Exception {
                System.out.println("processing item " + item);
                if (item.equalsIgnoreCase("26")) {
                    attemptCount++;
                    if (attemptCount >= 3) {
                        System.out.println("Retried " + attemptCount + " times success.");
                        return String.valueOf(Integer.valueOf(item) * -1);
                    } else {
                        System.out.println("Processed the " + attemptCount + " times fail.");
                        throw new CustomRetryException("Process failed. Attempt: " + attemptCount + " times.");
                    }
                } else {
                    return String.valueOf(Integer.valueOf(item) * -1);
                }

            }
        };

        return processor;
    }

    class CustomRetryException extends Exception {
        public CustomRetryException() {}

        public CustomRetryException(String msg) {
            super(msg);
        }
    }

}


