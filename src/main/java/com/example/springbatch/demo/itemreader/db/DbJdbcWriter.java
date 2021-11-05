package com.example.springbatch.demo.itemreader.db;

import com.example.springbatch.entity.User;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

//@Component(value = "dbJdbcWriter")
public class DbJdbcWriter implements ItemWriter<User> {
    @Override
    public void write(List<? extends User> list) throws Exception {
        list.forEach(System.out::println);
    }
}
