package io.quarkiverse.jms.runtime;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Session;

/**
 * A {@link ConnectionFactory} wrapper that creates {@link JmsContextWrapper}
 * instances using JMS 1.1 {@link Connection} and {@link Session}, for providers
 * that do not support the JMS 2.0 {@link JMSContext} API.
 */
class JmsContextWrappingConnectionFactory implements ConnectionFactory {

    private final ConnectionFactory delegate;

    JmsContextWrappingConnectionFactory(ConnectionFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public Connection createConnection() throws JMSException {
        return delegate.createConnection();
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return delegate.createConnection(userName, password);
    }

    @Override
    public JMSContext createContext() {
        return createContext(Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        try {
            Connection connection = delegate.createConnection();
            return new JmsContextWrapper(connection, sessionMode);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return createContext(userName, password, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        try {
            Connection connection = delegate.createConnection(userName, password);
            return new JmsContextWrapper(connection, sessionMode);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }
}
