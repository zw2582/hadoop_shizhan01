package cn.itcase.bigdata.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class UserManagerProxy implements InvocationHandler{
	
	private Object target;
	
	public Object getProxy(Object target) {
		this.target = target;
		
		return Proxy.newProxyInstance(target.getClass().getClassLoader(), 
				target.getClass().getInterfaces(), this);
	}

	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		System.out.println("xxxxxxxxxxxxxxxxxxxx");
		
		Object result = method.invoke(target, args);
		
		System.out.println("gggggggggggggggggggggggg");
		
		return result;
	}

}
