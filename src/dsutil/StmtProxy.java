/*
 * Created on 2005-1-7
 */
package dsutil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * @author hl
 */
public class StmtProxy implements InvocationHandler {

    private static final Logger logger = Logger.getLogger(StmtProxy.class);
    private Statement target;
    private List<Object> params;
    private boolean needLogParams;
    private ConnectionProxy connectionProxy;
    private Throwable initStack;

    public StmtProxy(Statement target) {
        this(null, target);
    }

    StmtProxy(ConnectionProxy cp, Statement target) {
        super();
        if (cp != null) {
            this.connectionProxy = cp;
            initStack = new Throwable();
        }
        this.target = target;
        needLogParams = logger.isDebugEnabled()
                && (target instanceof PreparedStatement);
        if (needLogParams && (target instanceof PreparedStatement)) {
            params = new ArrayList<Object>();
        }
    }

    /**
     * 注意这里即使target是个Statement一样可以被代理，返回的是一个PreparedStatement
     * 的实例，不会出错，不过在这种情况下在返回值上面调用PreparedStatement才有的方法会 出运行时异常。
     */
    public static Statement getProxyObject(Statement target) {
        try {
            StmtProxy p = new StmtProxy(target);
            PreparedStatement ps = (PreparedStatement) Proxy.newProxyInstance(
                    StmtProxy.class.getClassLoader(),
                    new Class[] { PreparedStatement.class }, p);
            return ps;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        try {
            String methodName = method.getName();
            if (methodName.equals("close")) {
                if (connectionProxy != null) {
                    connectionProxy.getStmtProxys().remove(this);
                }
            }
            if (args != null && args.length >= 2
                    && methodName.startsWith("set")
                    && args[0] instanceof Integer) {
                if ("setNull".equals(methodName)) {
                    if (((Number) args[1]).intValue() == Types.NULL) {
                        logger.warn("Don't invoke ps.setNull(paramIndex, Types.NULL);",
                                new IllegalArgumentException());
                    }
                    if (needLogParams)
                        params.add("null");
                } else {
                    if (needLogParams)
                        params.add(args[1]);
                }
            }
            if (needLogParams && methodName.equals("addBatch")) {
                if (params.size() > 0) {
                    logParams();
                    params.clear();
                }
            }
            boolean isExec = methodName.startsWith("execute");
            long t = -1;
            if (isExec) {
                if (args != null && args.length > 0) {
                    logger.info(args[0]);// sql
                }
                if (needLogParams && params.size() > 0) {
                    logParams();
                    params.clear();
                }
                t = System.currentTimeMillis();
            }
            Object rt = method.invoke(target, args);
            if (isExec && logger.isInfoEnabled()) {
                StringBuffer sb = new StringBuffer("[").append(methodName)
                        .append("] ").append((System.currentTimeMillis() - t))
                        .append("ms");
                if (rt instanceof int[]) {
                    if (logger.isDebugEnabled()) {
                        sb.append(" - return: [");
                        int[] a = (int[]) rt;
                        if (a != null && a.length > 0) {
                            for (int i = 0; i < a.length; i++) {
                                sb.append(a[i]).append(", ");
                            }
                            sb.deleteCharAt(sb.length() - 1);
                            sb.deleteCharAt(sb.length() - 1);
                        }
                        sb.append(']');
                    }
                } else {
                    sb.append(" - return: ").append(rt);
                }
                logger.info(sb.toString());
            }
            return rt;
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private void logParams() {
        StringBuffer sb = new StringBuffer();
        sb.append("[params] [");
        for (int i = 0; i < params.size(); i++) {
            sb.append(params.get(i));
            sb.append(',');
        }
        sb.setCharAt(sb.length() - 1, ']');
        logger.debug(sb.toString());
    }

    public Throwable getInitStack() {
        return initStack;
    }
}