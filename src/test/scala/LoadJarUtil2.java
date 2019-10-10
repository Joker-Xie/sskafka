/**
 * Created by LUJH13 on 2019-3-1.
 */

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;


public class LoadJarUtil2 {

    private static final Logger logger = LoggerFactory.getLogger(LoadJarUtil2.class.getName());

    public static void main (String[] args){
        loadJar("") ;
    }


    public static void loadJar(String jarPath) {

        URL url = null;
        // 获取jar的真实路径，过滤掉标志前缀(本地路径以"local|"标志，远程路径以"remote|"标志，s3路径以"s3|"标志)
        String realJarPath = "D:/mytest/mytest-1.0-SNAPSHOT.jar";
        File jarFile = new File(realJarPath);
        try {
            url = jarFile.toURI().toURL();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        if(null != url){
            // 从URLClassLoader类中获取类所在文件夹的方法，jar也可以认为是一个文件夹
            Method method = null;
            try {
                method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            } catch (NoSuchMethodException e) {
                logger.error("get method exception, jarPath:" + jarPath, e);
            }

            // 获取方法的访问权限以便写回
            boolean accessible = method.isAccessible();
            try {
                method.setAccessible(true);

                // 获取系统类加载器
                URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                method.invoke(classLoader, url);
            } catch (Exception e) {
                logger.error("load url to classLoader exception, jarPath:" + jarPath, e);
            } finally {
                method.setAccessible(accessible);
            }
            testInvokeMethod();
        }
    }


    public static void testInvokeMethod(){
        String fullClassName = "com.midea.test.Test1";
        Class<?> clazz = null;
        try {
            clazz = Class.forName(fullClassName);
            if(null != clazz){
                Object instance = clazz.newInstance();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

    }



}
