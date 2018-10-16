package com.bonc.bdevutils.com.bonc.bdev.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * @Author G.Goe
 * @Date 2018/10/16
 * @Request
 * @Resource
 */
public class HbaseConnectionFactory {

    private static Logger logger = LoggerFactory.getLogger(HbaseConnectionFactory.class);

    private Configuration configuration;

    public HbaseConnectionFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * 获取不需要认证的Connection
     *
     * @return
     * @throws IOException
     */
    public Connection getNonAuthenticationConnection() throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }

    /**
     * 获取需要Kerberos认证的Connection
     *
     * @param userPrincipal - 用户凭证
     * @param keytabPath    - keytab文件
     * @return
     * @throws IOException
     */
    public Connection getAuthenticationConnection(String userPrincipal, String keytabPath) throws IOException {
        Connection connection = null;

        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(userPrincipal, keytabPath);
        UserGroupInformation.setLoginUser(ugi);

        boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
        // 判断ugi是否开启了安全认证
        if (securityEnabled) {
            System.err.println("配置Kerberos，开启安全认证，进入认证通道获取连接！");
            connection = ugi.doAs(new PrivilegedAction<Connection>() {
                public Connection run() {
                    try {
                        return ConnectionFactory.createConnection(configuration);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), "Obtain securerity connection failed!");
                        return null;
                    }
                }
            });
        } else {
            System.err.println("不配置Kerberos，无法进入认证通道！不通过认证通过连接");
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }
}
