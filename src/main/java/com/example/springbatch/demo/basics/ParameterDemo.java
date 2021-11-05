package com.example.springbatch.demo.basics;

import com.example.springbatch.demo.BaseConfig;
import org.springframework.batch.core.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;

import java.util.Map;

//@Configuration
public class ParameterDemo extends BaseConfig implements StepExecutionListener {

    private Map<String, JobParameter> parameters;

    @Bean
    public Job parameterDemoJob() {
        return jobBuilderFactory.get("parameterDemoJob")
                .start(parameterStep())
                .build();
    }

    // Provid parameters to Step through listener
    @Bean
    public Step parameterStep() {
        return stepBuilderFactory.get("parameterStep")
                .listener(this)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("info: " + parameters.get("info"));
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        parameters = stepExecution.getJobParameters().getParameters();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }
}
