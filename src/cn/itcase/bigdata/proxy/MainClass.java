package cn.itcase.bigdata.proxy;

import java.lang.reflect.Method;

import org.junit.Test;

public class MainClass {

	/**
	 * @param args
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		IUserManager usermanger = (IUserManager) UserManagerFactory.getInstance(UserManagerImpl.class.getName());
		
		System.out.println("i begin to play");
		
		usermanger.addUser();
		usermanger.editUser();
		usermanger.findUser();
		usermanger.delUser();
		
		System.out.println("i am over");
	}
	
	@Test
	public void testReflect() throws Exception {
		Class<?> cla = Class.forName("cn.itcase.bigdata.proxy.IUserManager");
		Method[] methods = cla.getMethods();
		
		Method method = cla.getMethod("addUser", null);
		Object result = method.invoke(new UserManagerImpl(), null);
		System.out.println(result.toString());
	}

}
