package com.example.springbatch.demo.itemreader.file;

import com.example.springbatch.entity.Customer;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

//@Component("flatFileWriter")
public class FlatFileWriter implements ItemWriter<Customer> {
    @Override
    public void write(List<? extends Customer> list) throws Exception {
        list.forEach(System.out::println);
    }
}
