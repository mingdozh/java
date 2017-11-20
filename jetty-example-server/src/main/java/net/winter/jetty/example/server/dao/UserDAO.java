package net.winter.jetty.example.server.dao;

import org.apache.ibatis.session.SqlSession;

import net.winter.jetty.example.server.model.UserModel;

public class UserDAO {

	private BaseDAO baseDAO;

	public void setBaseDAO(BaseDAO baseDAO) {
		this.baseDAO = baseDAO;
	}

	private SqlSession openSession() {
		return baseDAO.getSqlSessionFactory().openSession();
	}

	public UserModel queryUserById(int id) {
		SqlSession sqlSession = openSession();

		try {
			return sqlSession.selectOne("net.winter.jetty.example.server.mapper.UserMapper.queryUserById", id);

		} finally {
			sqlSession.close();
		}
	}

	public int insertUser(UserModel user) {

		SqlSession sqlSession = openSession();

		try {
			sqlSession.insert("net.winter.jetty.example.server.mapper.UserMapper.insertUser", user);

			sqlSession.commit();

			return user.getId();

		} finally {
			sqlSession.close();
		}
	}

}
