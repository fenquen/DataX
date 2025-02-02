package com.fenquen.datax.distribute.dispatcher;

import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.common.util.ExecuteMode;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class BusinessController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessController.class);

    @Value("${json.dir}")
    private String jsonDir;

    @Value("${script.path}")
    private String scriptPath;

    @Value("${server.address}")
    private String serverAddress;

    @Value("${server.port}")
    private String serverPort;

    /**
     * @param json                          datax的json配置
     * @param jobIdStr                      datax体系的jobid
     * @param mode                          datax体系的mode 如果用户想要分布式调度使用增加的distribute
     * @param masterNodeHost                该node接收其它node调度来的task group时使用 普通用户无需理会
     * @param masterNodePort                该node接收其它node调度来的task group时使用 普通用户无需理会
     * @param masterNodeNettyHttpServerPort 该node接收其它node调度来的task group时使用 普通用户无需理会
     * @throws Exception
     */
    @RequestMapping(Constant.SPRING_HTTP.START_HTTP_PATH)
    public void start(String json,
                      @RequestParam(Constant.COMMAND_PARAM.jobid) String jobIdStr,
                      @RequestParam(Constant.COMMAND_PARAM.mode) ExecuteMode mode,
                      @RequestParam(required = false, value = Constant.ENV_PARAM.masterNodeHost) String masterNodeHost,
                      @RequestParam(required = false, value = Constant.ENV_PARAM.masterNodePort) String masterNodePort,
                      @RequestParam(required = false, value = Constant.ENV_PARAM.masterNodeNettyHttpServerPort) String masterNodeNettyHttpServerPort) throws Exception {
        File jsonFile = new File(jsonDir, jobIdStr + ".json");
        Files.write(Paths.get(jsonFile.getAbsolutePath()), json.getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        List<String> commandList = new ArrayList<>();
        commandList.add("/bin/bash");
        commandList.add(scriptPath);
        commandList.add(jsonFile.getAbsolutePath());
        commandList.add(jobIdStr);
        commandList.add(mode.name());

        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        Map<String, String> envs = processBuilder.environment();

        envs.put(Constant.ENV_PARAM.localNodeHost, serverAddress);
        envs.put(Constant.ENV_PARAM.localNodePort, serverPort);
        switch (mode) {
            case taskGroup:
                envs.put(Constant.ENV_PARAM.masterNodeHost, masterNodeHost);
                envs.put(Constant.ENV_PARAM.masterNodePort, masterNodePort);
                envs.put(Constant.ENV_PARAM.masterNodeNettyHttpServerPort, masterNodeNettyHttpServerPort);
                break;
            case distribute:
                envs.put(Constant.ENV_PARAM.masterNodeHost, serverAddress);
                envs.put(Constant.ENV_PARAM.masterNodePort, serverPort);
                envs.put(Constant.ENV_PARAM.nodeList, JSON.toJSONString(Global.HOST_PORT_DISPATCHER_INFO.values()));
                envs.put(Constant.ENV_PARAM.masterNodeNettyHttpServerPort, "8181");
                break;
            case local:
            case standalone:
                break;
            default:
                throw new DispatcherException("");
        }

        // processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        new Thread(() -> {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while (true) {
                String line;
                try {
                    line = bufferedReader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (line == null) {
                    break;
                }
                System.out.println(line);
            }

            try {
                process.waitFor();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Global.JOB_ID_PROCESS.remove(jobIdStr);
        }).start();


        Global.JOB_ID_PROCESS.put(jobIdStr, process);

        // System.out.println(process.waitFor());
    }

    /**
     * 需要区分本node对于这个jobid来说是总的还是小弟
     * 如果是小弟的话 那么直接杀死process 如果是总的话不能直接这么
     */
    @RequestMapping(Constant.SPRING_HTTP.STOP_HTTP_PATH)
    public void stop(@RequestParam(Constant.COMMAND_PARAM.jobid) String jobIdStr) throws Exception {
        LOGGER.info("调用 http stop ,jobid:{}", jobIdStr);

        Process process = Global.JOB_ID_PROCESS.remove(jobIdStr);
        if (process == null) {
            LOGGER.info("http stop,jobid:{}对应的process已不存在", jobIdStr);
            return;
        }

        int pid = getPid(process);
        LOGGER.info("http stop,jobid:{}对应的pid是 {}", jobIdStr, pid);

        Runtime.getRuntime().exec("kill -15 " + pid);
    }

    private int getPid(Process process) throws Exception {
        Class<?> clazz = Class.forName("java.lang.UNIXProcess");
        Field field = clazz.getDeclaredField("pid");
        field.setAccessible(true);
        return field.getInt(process);
    }
}
