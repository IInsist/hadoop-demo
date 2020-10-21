package com.hadoop.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class HadoopDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(HadoopDemoApplication.class, args);
	}

}
