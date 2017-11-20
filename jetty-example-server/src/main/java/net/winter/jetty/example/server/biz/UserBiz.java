package net.winter.jetty.example.server.biz;

import java.sql.Timestamp;

import net.winter.jetty.example.server.dao.UserDAO;
import net.winter.jetty.example.server.model.UserModel;
import net.winter.jetty.example.server.resource.User;

public class UserBiz {
	private UserDAO userDAO;

	public void setUserDAO(UserDAO userDAO) {
		this.userDAO = userDAO;
	}

	public User getUserById(int id) {
		UserModel model = userDAO.queryUserById(id);
		if (model == null)
			return null;
		return buildUserFromUserModel(model);
	}

	public int insertUser(User user) {
		Timestamp current = new Timestamp(System.currentTimeMillis());

		UserModel model = buildUserModelFromUser(user);
		model.setCreateTime(current);
		model.setModifyTime(current);

		return userDAO.insertUser(model);
	}

	private User buildUserFromUserModel(UserModel model) {
		User user = new User();

		user.setId(model.getId());
		user.setAge(model.getAge());
		user.setName(model.getName());
		user.setSex(model.getSex());

		return user;
	}

	private UserModel buildUserModelFromUser(User user) {
		UserModel model = new UserModel();

		model.setAge(user.getAge());
		model.setName(user.getName());
		model.setSex(user.getSex());
		model.setId(user.getId());

		return model;
	}
}
