package io.quarkiverse.it.jms.ibmmq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.XAConnectionFactory;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.ibm.msg.client.jakarta.jms.JmsConnectionFactory;
import com.ibm.msg.client.jakarta.jms.JmsFactoryFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;

@ApplicationScoped
public class IbmMqConnectionFactoryProducer {

    @Produces
    @Singleton
    ConnectionFactory connectionFactory(
            @ConfigProperty(name = "ibmmq.host") String host,
            @ConfigProperty(name = "ibmmq.port") int port,
            @ConfigProperty(name = "ibmmq.queue-manager", defaultValue = "QM1") String queueManager,
            @ConfigProperty(name = "ibmmq.channel", defaultValue = "DEV.APP.SVRCONN") String channel,
            @ConfigProperty(name = "ibmmq.user", defaultValue = "app") String user,
            @ConfigProperty(name = "ibmmq.password", defaultValue = "passw0rd") String password) throws Exception {
        JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.JAKARTA_WMQ_PROVIDER);
        var cf = ff.createConnectionFactory();
        configureFactory(cf, host, port, queueManager, channel, user, password);
        return cf;
    }

    @Produces
    @Singleton
    XAConnectionFactory xaConnectionFactory(
            @ConfigProperty(name = "ibmmq.host") String host,
            @ConfigProperty(name = "ibmmq.port") int port,
            @ConfigProperty(name = "ibmmq.queue-manager", defaultValue = "QM1") String queueManager,
            @ConfigProperty(name = "ibmmq.channel", defaultValue = "DEV.APP.SVRCONN") String channel,
            @ConfigProperty(name = "ibmmq.user", defaultValue = "app") String user,
            @ConfigProperty(name = "ibmmq.password", defaultValue = "passw0rd") String password) throws Exception {
        JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.JAKARTA_WMQ_PROVIDER);
        var cf = ff.createXAConnectionFactory();
        configureFactory(cf, host, port, queueManager, channel, user, password);
        return cf;
    }

    private void configureFactory(JmsConnectionFactory cf, String host, int port,
            String queueManager, String channel, String user, String password) throws Exception {
        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, host);
        cf.setIntProperty(WMQConstants.WMQ_PORT, port);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, queueManager);
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, channel);
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setIntProperty(WMQConstants.WMQ_SHARE_CONV_ALLOWED, WMQConstants.WMQ_SHARE_CONV_ALLOWED_YES);
        cf.setStringProperty(WMQConstants.USERID, user);
        cf.setStringProperty(WMQConstants.PASSWORD, password);
        cf.setIntProperty(WMQConstants.WMQ_SHARE_CONV_ALLOWED, WMQConstants.WMQ_SHARE_CONV_ALLOWED_YES);
    }
}
