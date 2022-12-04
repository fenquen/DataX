package com.fenquen.datax.distribute.dispatcher;

import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.common.util.ExecuteMode;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class BusinessController {

    @Value("${json.dir}")
    private String jsonDir;

    @Value("${script.path}")
    private String scriptPath;

    @Value("${server.address}")
    private String serverAddress;

    @Value("${server.port}")
    private String serverPort;

    @RequestMapping(Constant.START_HTTP_PATH)
    public void start(String json, @RequestParam("jobid") String jobIdStr, ExecuteMode mode) throws Exception {
        File jsonFile = new File(jsonDir, jobIdStr + ".json");
        Files.write(Paths.get(jsonFile.getAbsolutePath()), json.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        List<String> commandList = new ArrayList<>();
        commandList.add("/bin/bash");
        commandList.add(scriptPath);
        commandList.add(jsonFile.getAbsolutePath());
        commandList.add(jobIdStr);
        commandList.add(mode.name());

        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        Map<String, String> envs = processBuilder.environment();

        if (ExecuteMode.distribute.equals(mode)) {
            envs.put(Constant.ENV_PARAM.masterNodeHost, serverAddress);
            envs.put(Constant.ENV_PARAM.masterNodePort, serverPort);
            envs.put(Constant.ENV_PARAM.nodeList, JSON.toJSONString(Global.HOST_PORT_DISPATCHER_INFO.values()));
            envs.put(Constant.ENV_PARAM.masterNodeNettyHttpServerPort, "8181");
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
        }).start();
        // System.out.println(process.waitFor());
    }

    @RequestMapping("/stop")
    public void stop() {

    }
}
