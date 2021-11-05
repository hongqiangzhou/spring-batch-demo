package com.example.springbatch.demo.listener;

import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.BeforeChunk;
import org.springframework.batch.core.scope.context.ChunkContext;

//@Component(value = "myChunkListener")
public class MyChunkListener {

    @BeforeChunk
    public void beforeChunk(ChunkContext context) {
        System.out.println(context.getStepContext().getStepName() + " before...");
    }

    @AfterChunk
    public void afterChunk(ChunkContext context) {
        System.out.println(context.getStepContext().getStepName() + " after...");
    }
}
