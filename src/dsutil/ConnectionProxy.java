/*
 * Created on 2005-1-7
 */
package dsutil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author hl
 */
public class ConnectionProxy implements InvocationHandler {

	private static final Logger logger = Logger
			.getLogger(ConnectionProxy.class);
	private static final Map<String, Handler> map = new HashMap<String, Handler>();

	private Connection target;

	private String hash;

	private Throwable initStack;

	private List<StmtProxy> stmtProxys = new ArrayList<StmtProxy>();

	public ConnectionProxy(Connection target) {
		super();
		this.target = target;
		this.hash = "(" + Integer.toHexString(target.hashCode()) + ") ";
		initStack = new Throwable();
	}

	public static Connection getProxyObject(Connection target) {
		try {
			ConnectionProxy p = new ConnectionProxy(target);
			Connection con = (Connection) Proxy.newProxyInstance(
					ConnectionProxy.class.getClassLoader(),
					new Class[] { Connection.class }, p);
			return con;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		try {
			String name = method.getName();
			Handler h = map.get(name);
			if (h == null) {
				return method.invoke(target, args);
			} else {
				return h.invoke(this, proxy, method, args);
			}
		} catch (InvocationTargetException e) {
			throw e.getTargetException();
		}
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (!target.isClosed()) {
			logger.error(hash + " Connection not closed, call stack :",
					initStack);
		}
	}

	private static interface Handler {
		public Object invoke(ConnectionProxy parent, Object proxy,
				Method method, Object[] args) throws Throwable;
	}

	private static Handler handlerStmt = new Handler() {
		public Object invoke(ConnectionProxy parent, Object proxy,
				Method method, Object[] args) throws Throwable {
			if (logger.isInfoEnabled() && args != null && args.length > 0) {
				logger.info(parent.hash + args[0]);// sql
			}
			Object rt = method.invoke(parent.target, args);
			StmtProxy p = new StmtProxy(parent, (Statement) rt);
			// 不加判断可能出现强制类型转换异常
			if (rt instanceof CallableStatement) {
				CallableStatement cs = (CallableStatement) Proxy
						.newProxyInstance(StmtProxy.class.getClassLoader(),
								new Class[] { CallableStatement.class }, p);
				parent.stmtProxys.add(p);
				return cs;
			} else if (rt instanceof PreparedStatement) {
				PreparedStatement ps = (PreparedStatement) Proxy
						.newProxyInstance(StmtProxy.class.getClassLoader(),
								new Class[] { PreparedStatement.class }, p);
				parent.stmtProxys.add(p);
				return ps;
			} else {
				Statement s = (Statement) Proxy.newProxyInstance(
						StmtProxy.class.getClassLoader(),
						new Class[] { Statement.class }, p);
				parent.stmtProxys.add(p);
				return s;
			}
		}
	};

	private static Handler handlerRollback = new Handler() {
		public Object invoke(ConnectionProxy parent, Object proxy,
				Method method, Object[] args) throws Throwable {
			if (logger.isInfoEnabled()) {
				logger.info(parent.hash + " Transaction rollbacked.");
			}
			return method.invoke(parent.target, args);
		}
	};

	private static Handler handlerClose = new Handler() {
		public Object invoke(ConnectionProxy parent, Object proxy,
				Method method, Object[] args) throws Throwable {
			if (logger.isDebugEnabled()) {
				logger.debug(parent.hash + " Collection closed.");
			}
			if (parent.stmtProxys.size() > 0) {
				for (StmtProxy sp : parent.stmtProxys) {
					logger.error(parent.hash
							+ " Statment not closed, call stack:", sp
							.getInitStack());
				}
			}
			return method.invoke(parent.target, args);
		}
	};

	private static Handler handlerCommit = new Handler() {
		public Object invoke(ConnectionProxy parent, Object proxy,
				Method method, Object[] args) throws Throwable {
			if (logger.isDebugEnabled()) {
				logger.debug(parent.hash + " Transaction committed");
			}
			return method.invoke(parent.target, args);
		}
	};

	private static Handler handlerSetAutoCommit = new Handler() {
		public Object invoke(ConnectionProxy parent, Object proxy,
				Method method, Object[] args) throws Throwable {
			if (logger.isDebugEnabled()) {
				if (args[0].equals(Boolean.TRUE)) {
					logger.debug(parent.hash + " Set auto commit");
				} else {
					logger.debug(parent.hash + " Set manually commit");
				}
			}
			return method.invoke(parent.target, args);
		}
	};

	private static Handler handlerSetTransactionIsolation = new Handler() {
		public Object invoke(ConnectionProxy parent, Object proxy,
				Method method, Object[] args) throws Throwable {
			if (logger.isDebugEnabled()) {
				String iso = getTransactionIsolationName((Number) args[0]);
				logger.debug(parent.hash + " Set transaction isolation level: "
						+ iso);
			}
			return method.invoke(parent.target, args);
		}

		private String getTransactionIsolationName(Number x) {
			switch (x.intValue()) {
			case Connection.TRANSACTION_NONE:
				return "TRANSACTION_NONE";
			case Connection.TRANSACTION_READ_COMMITTED:
				return "TRANSACTION_READ_COMMITTED";
			case Connection.TRANSACTION_READ_UNCOMMITTED:
				return "TRANSACTION_READ_UNCOMMITTED";
			case Connection.TRANSACTION_REPEATABLE_READ:
				return "TRANSACTION_REPEATABLE_READ";
			case Connection.TRANSACTION_SERIALIZABLE:
				return "TRANSACTION_SERIALIZABLE";
			default:
				return "UNKNOWN";
			}
		}
	};

	static {
		map.put("prepareStatement", handlerStmt);
		map.put("prepareCall", handlerStmt);
		map.put("createStatement", handlerStmt);
		map.put("rollback", handlerRollback);
		map.put("close", handlerClose);
		map.put("commit", handlerCommit);
		map.put("setAutoCommit", handlerSetAutoCommit);
		map.put("setTransactionIsolation", handlerSetTransactionIsolation);
	}

	public List<StmtProxy> getStmtProxys() {
		return stmtProxys;
	}

	public void setStmtProxys(List<StmtProxy> stmts) {
		this.stmtProxys = stmts;
	}

}