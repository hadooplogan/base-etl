package com.chinadaas.common.util;

import com.google.common.io.Resources;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author guzhandong
 * @CREATE 2017-04-25 9:47 PM
 */

public class LogUtil {

    private Logger logger;

    public static LogUtil getLogger (Logger logger) {
        return  new LogUtil(logger);
    }

    public static LogUtil getLogger (Class<?> clazz) {
        return  new LogUtil(clazz);
    }


    public LogUtil(Logger logger) {
        this.logger = logger;
    }
    public LogUtil(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    public void info(String msg, Object ... args){
        if (logger.isInfoEnabled())
            logger.info(msg , args);
    }
    public void debug(String msg,Object ... args){
        if (logger.isDebugEnabled())
            logger.debug(msg , args);
    }
    public void error(String msg,Object ... args){
        if (logger.isErrorEnabled())
            logger.error(msg , args);
    }
    public void warn(String msg,Object ... args){
        if (logger.isWarnEnabled())
            logger.warn(msg , args);
    }
}
