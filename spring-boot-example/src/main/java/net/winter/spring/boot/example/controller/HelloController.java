package net.winter.spring.boot.example.controller;

import net.winter.spring.boot.example.GreetingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@RestController
public class HelloController {
	private static final Logger logger = LoggerFactory.getLogger(HelloController.class);
	private static AtomicLong counter = new AtomicLong(0);

	@GetMapping(path = "/index")
	public GreetingResponse index(@RequestParam(required = true) String name) {
		logger.debug("Enter index:{}",name);
		GreetingResponse res =  new GreetingResponse();
		res.setDesc("Hello,"+ name);
		res.setId(counter.getAndIncrement());
		return res;
	}

	@GetMapping(path="/index/{name}")
	public GreetingResponse hello(@PathVariable String name){
        logger.debug("Enter hello:{}",name);
        GreetingResponse res =  new GreetingResponse();
        res.setDesc("Hello,"+name);
        res.setId(counter.getAndIncrement());
        return res;
    }
}