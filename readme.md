# datax分布式化
## 说明
开源的非商业版本的datax是没有分布式调度能力的，对datax的原始代码进行了较为深入的研究后动手更改

分布下的各个节点基于组播机制互相发现，非中心化各节点之间互相对等

初步实现了分片读取rdbms(使用splitPk)下的分布式调度

调度粒度是单个的task group，多的task group中的task会调度到其它node上


## 如何使用
新增了dispatcher的module，它会启动1个http服务用来接收任务请求同时也在后台不断的向组播地址发送上报自身信息


```
/**
     * @param json datax的json配置
     * @param jobIdStr datax体系的jobid 
     * @param mode datax体系的mode 如果用户想要分布式调度使用增加的distribute
     * @param masterNodeHost 该node接收其它node调度来的task group时使用 普通用户无需理会
     * @param masterNodePort 该node接收其它node调度来的task group时使用 普通用户无需理会
     * @param masterNodeNettyHttpServerPort 该node接收其它node调度来的task group时使用 普通用户无需理会
     * @throws Exception
     */
    @RequestMapping(Constant.SPRING_HTTP.START_HTTP_PATH)
    public void start(String json,
                      @RequestParam(Constant.COMMAND_PARAM.jobid) String jobIdStr,
                      @RequestParam(Constant.COMMAND_PARAM.mode) ExecuteMode mode,
                      @RequestParam(required = false, value = Constant.ENV_PARAM.masterNodeHost) String masterNodeHost,
                      @RequestParam(required = false, value = Constant.ENV_PARAM.masterNodePort) String masterNodePort,
                      @RequestParam(required = false, value = Constant.ENV_PARAM.masterNodeNettyHttpServerPort) String masterNodeNettyHttpServerPort) throws Exception
```