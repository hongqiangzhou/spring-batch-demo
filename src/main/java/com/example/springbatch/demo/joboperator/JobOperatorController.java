package com.example.springbatch.demo.joboperator;

import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.batch.core.launch.JobOperator;
import java.util.Properties;

@RestController
public class JobOperatorController {

    @Autowired
    private JobOperator jobOperator;

    @GetMapping(value = "/job2/{msg}")
    public String runJob2(@PathVariable String msg) throws JobInstanceAlreadyExistsException, NoSuchJobException, JobParametersInvalidException {
        jobOperator.start("jobOperatorDemoJob", "msg=" + msg);
        return "Job success.";
    }
}
