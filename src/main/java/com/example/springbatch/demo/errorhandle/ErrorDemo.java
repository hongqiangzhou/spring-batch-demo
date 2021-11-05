package com.example.springbatch.demo.errorhandle;

import com.example.springbatch.demo.BaseConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;

import java.util.Map;

//@Configuration
public class ErrorDemo extends BaseConfig {

    @Bean
    public Job errorsDemoJob() {
        return jobBuilderFactory.get("errorsDemoJob")
                .start(errorStep1())
                .next(errorStep2())
                .build();
    }

    @Bean
    public Step errorStep1() {
        return stepBuilderFactory.get("errorStep1")
                .tasklet(errorHandling())
                .build();
    }

    @Bean
    public Step errorStep2() {
        return stepBuilderFactory.get("errorStep2")
                .tasklet(errorHandling())
                .build();
    }

    @Bean
    @StepScope
    public Tasklet errorHandling() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                Map<String, Object> stepExecutionContext = chunkContext.getStepContext().getStepExecutionContext();

                if (stepExecutionContext.containsKey("qianfeng")) {
                    System.out.println("The second run will succeed.");
                    return RepeatStatus.FINISHED;
                } else {
                    System.out.println("The first run will fail.");
                    chunkContext.getStepContext().getStepExecution().getExecutionContext().put("qianfeng", true);
                    throw new RuntimeException("error...");
                }
            }
        };
    }


}
