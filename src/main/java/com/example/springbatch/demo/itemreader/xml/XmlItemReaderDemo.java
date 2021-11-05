package com.example.springbatch.demo.itemreader.xml;

import com.example.springbatch.demo.BaseConfig;
import com.example.springbatch.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//@Configuration
public class XmlItemReaderDemo extends BaseConfig {

    @Bean
    public Job xmlItemReaerDemoJob() {
        return jobBuilderFactory.get("xmlItemReaerDemoJob")
                .start(xmlItemReaderDemoStep())
                .build();
    }

    @Bean
    public Step xmlItemReaderDemoStep() {
        return stepBuilderFactory.get("xmlItemReaderDemoStep")
                .<Customer, Customer>chunk(2)
                .reader(xmlFileReader())
                .writer(xmlFileWriter())
                .build();
    }

    @Bean
    public StaxEventItemReader<? extends Customer> xmlFileReader() {
        StaxEventItemReader<Customer> reader = new StaxEventItemReader<>();
        reader.setResource(new ClassPathResource("customer.xml"));
        reader.setFragmentRootElementName("customer");

        XStreamMarshaller unmarshaller = new XStreamMarshaller();
        Map<String, Class> map = new HashMap<>(){{
            put("customer", Customer.class);
        }};
        unmarshaller.setAliases(map);

        reader.setUnmarshaller(unmarshaller);
        return reader;
    }

    @Bean
    public ItemWriter<? super Customer> xmlFileWriter() {
        ItemWriter<Customer> writer = new ItemWriter<Customer>() {
            @Override
            public void write(List<? extends Customer> list) throws Exception {
                list.forEach(System.out::println);
            }
        };

        return writer;
    }

}
