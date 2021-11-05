package com.example.springbatch.demo.itemwriter.basic;

import org.springframework.batch.item.ItemWriter;

import java.util.List;

//@Component(value = "myWriter")
public class MyWriter implements ItemWriter<String> {
    @Override
    public void write(List<? extends String> list) throws Exception {
        System.out.println(list.size());
        list.forEach(System.out::println);
    }
}
