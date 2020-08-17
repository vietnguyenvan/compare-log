package com.hiko.audit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class AuditService {

    private final String datePattern;

    private static Logger logger = LogManager.getLogger(AuditService.class);

    private static final long MINUTE = 60000;

    public AuditService(@Value("${app.log.date.pattern}") String datePattern) {
        this.datePattern = datePattern;
    }

    public void auditThreeLogByTime(File inputLogDir, File processLogDir, File outputLogDir, long from, long to) throws IOException {
//        List<String> inputLog = readFile(inputLogDir, from, to, destLogs);
//        List<String> processLog = readFile(processLogDir, from, to + MINUTE * 10, destLogs);
//        List<String> outputLog = readFile(outputLogDir, from, to + MINUTE * 15, destLogs);
//        compareThreeLogs(inputLog, processLog, outputLog);
    }

    public void auditTwoLogByTime(File sourceLogDir, File destLogDir, long from, long to) throws IOException {
         AtomicReference<List<String>> sourceLogs = new AtomicReference<>();
         AtomicReference<Map<String, String>> destLogs = new AtomicReference<>();
        CountDownLatch readLogFinish = new CountDownLatch(2);
        Thread sourceReaderThread = new Thread(() -> {
            logger.info("Reading file in {}", sourceLogDir);
            sourceLogs.set(readFile(sourceLogDir, from, to).collect(Collectors.toList()));
            logger.info("Ending reading source logs with size: {}", sourceLogs.get().size());
            readLogFinish.countDown();
        });
        sourceReaderThread.start();
        Thread destReaderThread = new Thread(() -> {
            logger.info("Reading file in {}", destLogDir);
            destLogs.set(readFile(destLogDir, from, to + MINUTE * 10).collect(Collectors.toMap(e -> e, e -> "")));
            logger.info("End of reading destination logs with size: {}", destLogs.get().size());
            readLogFinish.countDown();
        });
        destReaderThread.start();

//        new Thread(() -> {
////            logger.info("Remove match records ...");
////            while (readLogFinish.getCount() != 0) {
////                removeDuplicate(sourceLogs, destLogs);
////                try {
////                    Thread.sleep(10);
////                } catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
////            }
////        }).start();

        try {
            readLogFinish.await();
            logger.info("start of comparing ");
            Map<String, String> dest = destLogs.get();
            List<String> source = sourceLogs.get();
            List<String> result = source.stream().filter(e -> !dest.containsKey(e)).collect(Collectors.toList());
            logger.info("end of comparing with {} missing request", result.size());
            FileWriter writer = new FileWriter("./output.txt", false);
            result.stream().forEach(e -> {
                try {
                    writer.write("Process log miss request with rid: " + e + "\n");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            });
            writer.close();
            logger.info("Finished ...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void removeDuplicate(List<String> source, List<String> dest) {
        if (source == null || source.size() == 0) return;
        List copySource = new CopyOnWriteArrayList(source);
        source.removeAll(dest);
        dest.removeAll(copySource);
    }


    private void compareThreeLogs(List<String> inputLog, List<String> processLog, List<String> outputLog) throws IOException {
        List<String> compare1 = List.copyOf(inputLog);
        List<String> compare2 = List.copyOf(inputLog);
        compare1.removeAll(processLog);
        compare1.removeAll(outputLog);
        FileWriter writer = new FileWriter("./output.txt", false);
        if (compare1.size() != 0) {
            compare1.stream().forEach(e -> {
                try {
                    writer.write("Process log miss request with rid: " + e + "\n");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            });
        }

        if (compare2.size() != 0) {
            compare2.stream().forEach(e -> {
                try {
                    writer.write("Output log miss request with rid: " + e);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            });
        }
    }

    private void compareTwoLogs(List<String> source, List<String> dest) throws IOException {
        List<String> misMatches = source.parallelStream().filter(e -> !dest.contains(e)).collect(Collectors.toList());
        FileWriter writer = new FileWriter("./output.txt", false);
        if (misMatches.size() != 0) {
            misMatches.stream().forEach(e -> {
                try {
                    writer.write("Miss request with rid: " + e);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            });
        }
    }

    private Stream<String> readFile(File directory, long from, long to) {
        Collection<File> listFile = FileUtils.listFiles(directory, new String[]{"log"}, true);

        if (listFile == null || listFile.size() == 0) {
            new RuntimeException("No any log file in directory: " + directory.getAbsolutePath());
        }

        return listFile.parallelStream().map(file -> {
            List<String> logs = new LinkedList<>();
            logger.info("read file {}", file.getName());
            BufferedReader br = null;
            try {
                FileInputStream fstream = new FileInputStream(file);
                DataInputStream in = new DataInputStream(fstream);
                br = new BufferedReader(new InputStreamReader(in));
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
                String strLine;
                while ((strLine = br.readLine()) != null) {
                    Date time = getTime(strLine, simpleDateFormat);
                    if (time == null) continue;
                    if (to < time.getTime()) {
                        logger.info("Over range time: {}, file {}", time, file.getCanonicalPath());
                        break;
                    }
                    if (from <= time.getTime() && to >= time.getTime()) {
                        String rid = getRequestId(strLine);
                        if (rid == null) continue;
                        logs.add(getRequestId(strLine));
                    }
                }
                in.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null)
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
            return logs;
        }).flatMap(Collection::stream);
    }

    private String getRequestId(String line) {
        String msg = line.trim().substring(line.indexOf(" ", line.indexOf(" ") + 1));
        if (msg.indexOf("|") < 0) return null;
        return msg.substring(0, msg.indexOf("|"));
    }

    private Date getTime(String line, SimpleDateFormat simpleDateFormat) {
        String time = line.trim().substring(0, line.indexOf(" ", line.indexOf(" ") + 1));
        if (Strings.isEmpty(time)) return null;
        try {
            return simpleDateFormat.parse(time);
        } catch (Exception e) {
            System.out.println("Skip line due to invalid time : " + line);
        }
        return null;
    }
}
