package com.alibaba.datax.plugin.reader.ftpreader;

import java.io.InputStream;
import java.util.HashSet;
import java.util.List;

public abstract class FtpHelper {
    /**
     * 与ftp服务器建立连接
     */
    public abstract void login(String host,
                               String username,
                               String password,
                               int port,
                               int timeout,
                               String connectMode);

    /**
     * 断ftp服务器的连接
     */
    public abstract void logout();

    /**
     * 判断指定路径是否是目录
     */
    public abstract boolean isDirExist(String directoryPath);

    /**
     * 判断指定路径是否是文件
     */
    public abstract boolean isFileExist(String filePath);

    /**
     * 判断指定路径是否是软链接
     */
    public abstract boolean isSymbolicLink(String filePath);

    /**
     * 递归获取指定路径下符合条件的所有文件绝对路径
     */
    protected abstract HashSet<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel);

    /**
     * 获取指定路径的输入流
     */
    public abstract InputStream getInputStream(String filePath);

    /**
     * 获取指定路径列表下符合条件的所有文件的绝对路径
     */
    public HashSet<String> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel) {
        HashSet<String> sourceAllFiles = new HashSet<>();
        if (!srcPaths.isEmpty()) {
            for (String eachPath : srcPaths) {
                sourceAllFiles.addAll(getListFiles(eachPath, parentLevel, maxTraversalLevel));
            }
        }
        return sourceAllFiles;
    }

}
