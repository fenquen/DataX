package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.util.Configuration;

public interface Pluginable {
	String getDeveloper();

    String getDescription();

    void setPluginConf(Configuration pluginConf);

	void init();

	void destroy();

    String getPluginName();

    Configuration getPluginJobReaderWriterParamConf();

    Configuration getPeerPluginJobReaderWriterParamConf();

    public String getPeerPluginName();

    void setPluginJobReaderWriterParamConf(Configuration jobConf);

    void setPeerPluginJobReaderWriterParamConf(Configuration peerPluginJobConf);

    public void setPeerPluginName(String peerPluginName);

}
