package com.fenquen.datax.distribute.dispatcher;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@RestController
@RequestMapping("/business")
public class BusinessController {

    @Value("${json.dir}")
    private String jsonDir;

    @Value("${script.path}")
    private String scriptPath;

    @RequestMapping("/start")
    public void start(String json, int jobId, String mode) throws Exception {
        File jsonFile = new File(jsonDir, jobId + ".json");
        Files.write(Paths.get(jsonFile.getAbsolutePath()), json.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        ProcessBuilder processBuilder = new ProcessBuilder("/bin/bash", scriptPath, jsonFile.getAbsolutePath(), jobId + "", mode);
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
