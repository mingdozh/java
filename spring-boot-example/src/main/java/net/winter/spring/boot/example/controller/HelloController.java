package net.winter.spring.boot.example.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
public class HelloController {
	private static final Logger logger = LoggerFactory.getLogger(HelloController.class);

	@RequestMapping(path = "/")
	public String index() {
		logger.debug("Enter index:");
		return "Greetings from Spring Boot!";
	}

}