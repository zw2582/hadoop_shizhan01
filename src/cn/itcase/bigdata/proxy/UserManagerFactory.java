package cn.itcase.bigdata.proxy;

public class UserManagerFactory {
	
	private static IUserManager manager;

	public static Object getInstance(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class forName = Class.forName(className);
		Object instance =  forName.newInstance();
		
		instance = (IUserManager) new UserManagerProxy().getProxy(instance);
		
		return instance;
	}
}
