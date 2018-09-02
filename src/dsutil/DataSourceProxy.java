/*
 * Created on 2005-1-25
 */
package dsutil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * @author hl
 */
public class DataSourceProxy implements InvocationHandler {

    private static final Logger logger = Logger
            .getLogger(DataSourceProxy.class);
    private boolean debug = true;

    private DataSource target;
    private DataSource proxyDs;

    public DataSourceProxy(DataSource target) {
        super();
        this.target = target;
    }

    public DataSourceProxy() {
    }

    public DataSource getProxyObject() {
        try {
            if (proxyDs == null) {
                proxyDs = (DataSource) Proxy.newProxyInstance(
                        DataSourceProxy.class.getClassLoader(),
                        new Class[] { DataSource.class }, this);
            }
            return proxyDs;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        try {
            Object rt = method.invoke(target, args);
            if (debug && method.getName().equals("getConnection")) {
                String hash = null;
                boolean loggerDebugEnabled = logger.isDebugEnabled();
                if (loggerDebugEnabled) {
                    hash = "(" + Integer.toHexString(rt.hashCode()) + ") ";
                }
                rt = ConnectionProxy.getProxyObject((Connection) rt);
                if (loggerDebugEnabled) {
                    logger.debug(hash + " got a jdbc connection.");
                }
            }
            return rt;
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public DataSource getTarget() {
        return target;
    }

    public void setTarget(DataSource target) {
        this.target = target;
    }

}