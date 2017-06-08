package com.tgt.estore.druid;

import com.tgt.estore.druid.client.CustomClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@EnableConfigurationProperties
public class DruidApplication {

    @Value("${druid.host}")
    private String druidHost;

	public static void main(String[] args) {
		SpringApplication.run(DruidApplication.class, args);

		System.err.println("Successfully executed");
	}

    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    CustomClient druidClient(){
        return CustomClient.create(druidHost);
    }
}
