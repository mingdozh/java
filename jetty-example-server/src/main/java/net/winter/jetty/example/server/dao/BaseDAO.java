package net.winter.jetty.example.server.dao;

import java.io.IOException;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.springframework.core.io.ClassPathResource;

public class BaseDAO {

	private SqlSessionFactory sqlSessionFactory;

	public BaseDAO(String resource) {
		try {

			sqlSessionFactory = new SqlSessionFactoryBuilder().build(new ClassPathResource(resource).getInputStream());

		} catch (IOException e) {
			throw new IllegalArgumentException(String.format("Fail to create %s from : %s", BaseDAO.class.getName(), resource));
		}
	}

	public SqlSessionFactory getSqlSessionFactory() {
		return this.sqlSessionFactory;
	}
}
