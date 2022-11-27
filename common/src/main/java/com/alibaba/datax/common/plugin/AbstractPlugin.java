package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.util.Configuration;

import java.util.List;

public abstract class AbstractPlugin extends BaseObject implements Pluginable {
	// 作业的config DATAX_JOB_CONTENT_READER_PARAMETER 分割之前的
    private Configuration pluginJobReaderWriterParamConf;

    // 对端的作业configuration DATAX_JOB_CONTENT_WRITER_PARAMETER 分割之前的
    private Configuration peerPluginJobReaderWriterParamConf;

    // 插件本身的plugin
    private Configuration pluginConf;

    private String peerPluginName;

    @Override
	public String getPluginName() {
		assert null != this.pluginConf;
		return this.pluginConf.getString("name");
	}

    @Override
	public String getDeveloper() {
		assert null != this.pluginConf;
		return this.pluginConf.getString("developer");
	}

    @Override
	public String getDescription() {
		assert null != this.pluginConf;
		return this.pluginConf.getString("description");
	}

    @Override
	public Configuration getPluginJobReaderWriterParamConf() {
		return pluginJobReaderWriterParamConf;
	}

    @Override
	public void setPluginJobReaderWriterParamConf(Configuration pluginJobReaderParamConf) {
		this.pluginJobReaderWriterParamConf = pluginJobReaderParamConf;
	}

    @Override
	public void setPluginConf(Configuration pluginConf) {
		this.pluginConf = pluginConf;
	}

    @Override
    public Configuration getPeerPluginJobReaderWriterParamConf() {
        return peerPluginJobReaderWriterParamConf;
    }

    @Override
    public void setPeerPluginJobReaderWriterParamConf(Configuration peerPluginJobConf) {
        this.peerPluginJobReaderWriterParamConf = peerPluginJobConf;
    }

    @Override
    public String getPeerPluginName() {
        return peerPluginName;
    }

    @Override
    public void setPeerPluginName(String peerPluginName) {
        this.peerPluginName = peerPluginName;
    }

    public void preCheck() {
    }

	public void prepare() {
	}

	public void post() {
	}

    public void preHandler(Configuration jobConfiguration){

    }

    public void postHandler(Configuration jobConfiguration){

    }

    // 以下只是用在oss的reader writer
    private List<Configuration> readerPluginSplitConf;

    public List<Configuration> getReaderPluginSplitConf(){
        return readerPluginSplitConf;
    }

    public void setReaderPluginSplitConf(List<Configuration> readerPluginSplitConf){
        this.readerPluginSplitConf = readerPluginSplitConf;
    }
}
