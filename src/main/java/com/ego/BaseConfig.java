package com.ego;

public class BaseConfig {
    public static final String JOB_NAME = "config file";
    public static final String HOST = "localhost";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "root";
}


class DevConfig extends BaseConfig {
    public static final String JOB_NAME = "config file";
    public static final String HOST = "localhost";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "root";
}

class ProdConfig extends BaseConfig {
    public static final String JOB_NAME = "config file";
    public static final String HOST = "localhost";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "root";
}