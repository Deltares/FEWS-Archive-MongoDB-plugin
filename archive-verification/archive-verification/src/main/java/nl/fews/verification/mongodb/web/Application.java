package nl.fews.verification.mongodb.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration;
import org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import javax.security.auth.login.Configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SpringBootApplication(exclude = {MongoAutoConfiguration.class, DataMongoAutoConfiguration.class})
public class Application extends SpringBootServletInitializer {

	private static final Logger logger = LogManager.getLogger(Application.class);

	static{
		logger.info("{} Version: {}", Application.class.getSimpleName(), Application.class.getPackage().getImplementationVersion());
	}

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(Application.class);
    }

    public static void main(String[] args) {
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		Configuration.setConfiguration(new JaasConfiguration());
        SpringApplication.run(Application.class, args);
    }
}