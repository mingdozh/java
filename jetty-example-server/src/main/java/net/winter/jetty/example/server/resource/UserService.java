package net.winter.jetty.example.server.resource;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.core.MediaType;

import net.winter.jetty.example.server.biz.UserBiz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserService {
	private static final Logger logger = LoggerFactory.getLogger(UserService.class);

	private UserBiz userBiz;
	public void setUserBiz(UserBiz userBiz) {
		this.userBiz = userBiz;
	}

	@GET
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public User getUser(@PathParam("id") int id) {
		logger.info("get user by id:{}", id);

		User user = null;

		try {
			user = userBiz.getUserById(id);
		} catch (Exception e) {
			throw new ServiceUnavailableException();
		}

		if (user != null) {
			return user;
		}

		throw new NotFoundException(String.format("user %d can't be found", id));
	}

	@POST
	@Path("")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public User insert(User user) {
		try {
			user.setId(userBiz.insertUser(user));
		} catch (Exception e) {
			throw new ServiceUnavailableException();
		}

		return user;
	}

}
