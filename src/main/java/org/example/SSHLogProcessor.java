package org.example;
import com.jcraft.jsch.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SSHLogProcessor类用于通过SSH连接到远程服务器，读取日志文件，并处理日志内容。
 * 日志内容会被解析并写入CSV文件，无效的记录会被写入单独的文本文件。
 */
public class SSHLogProcessor {
    private static final int BUFFER_SIZE = 1013; // 固定缓冲区大小
    private static final int MAX_BUFFER_SIZE = 100 * 1024 * 1024; // 缓冲区最大限制（10MB）
    private static final int IO_BUFFER_SIZE = 30 * 1024 * 1024; // IO缓冲字符串最大限制（10MB）
    private static final int THREAD_POOL_SIZE = 1; // 线程池大小
    private static final int MAX_ITERATIONS = 10000; // 最大执行次数
    private static final int PRINT_MEMORY_INTERVAL = 1000; // 打印内存间隔
    private static final int MAX_RETRY_COUNT = 3; // 最大重试次数
    private static final int MAX_FIELD_LENGTH = 32767; // 最大字段长度


    /**
     * 主函数，程序的入口。
     *
     * @param args 命令行参数，依次为远程IP地址、端口号、用户名和密码。
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java SSHLogProcessor <remoteIP> <port> <username> <password>");
            return;
        }

        String remoteIP = args[0];
        int port = Integer.parseInt(args[1]);
        String username = args[2];
        String password = args[3];

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // 循环执行日志处理任务，直到达到最大执行次数
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            try {
                processLog(remoteIP, port, username, password, executor);
                System.out.println("日志处理完成 , 第" + (i + 1) + "次");
                if ((i + 1) % PRINT_MEMORY_INTERVAL == 0) {
                    printMemoryUsage();
                }
            } catch (Exception e) {
                System.out.println("日志处理失败 , 第" + (i + 1) + "次");
                e.printStackTrace();
            }
        }

        executor.shutdown();
    }

    /**
     * 通过SSH连接到远程服务器，读取日志文件并处理日志内容。
     *
     * @param remoteIP 远程服务器的IP地址
     * @param port 远程服务器的端口号
     * @param username SSH连接的用户名
     * @param password SSH连接的密码
     * @param executor 用于异步处理日志内容的线程池
     * @throws JSchException 如果SSH连接失败
     * @throws IOException 如果读取日志文件失败
     */
    private static void processLog(String remoteIP, int port, String username, String password, ExecutorService executor)
            throws JSchException, IOException {
        int retryCount = 0;
        // 循环重试
        while (true) {
            try {
                // 初始化JSch对象
                JSch jsch = new JSch();
                // 创建会话
                Session session = jsch.getSession(username, remoteIP, port);
                session.setPassword(password);
                // 设置不严格检查主机密钥
                session.setConfig("StrictHostKeyChecking", "no");
                // 连接会话
                session.connect();

                // 创建执行通道
                ChannelExec channel = (ChannelExec) session.openChannel("exec");
                // 设置要执行的命令
                channel.setCommand("cat test.log");
                // 设置输入流和错误流
                channel.setInputStream(null);
                channel.setErrStream(System.err);

                // 获取输出流
                InputStream in = channel.getInputStream();
                // 连接通道
                channel.connect();

                // 处理日志内容
                processLogContent(in, executor);

                // 断开通道和会话连接
                channel.disconnect();
                session.disconnect();
                break; // 成功执行后退出循环
            } catch (Exception e) {
                e.printStackTrace();
                // 判断是否达到最大重试次数
                if (retryCount < MAX_RETRY_COUNT) {
                    retryCount++;
                    System.out.println("连接失败，重试中... (" + retryCount + "/" + MAX_RETRY_COUNT + ")");
                }else {
                    System.out.println("重试失败，抛出异常");
                    throw e; // 达到最大重试次数后抛出异常
                }
            }
        }
    }


    /**
     * 处理日志内容，将有效记录写入CSV文件，无效记录写入文本文件。
     *
     * @param logStream 日志输入流，包含从远程服务器读取的日志数据
     * @param executor 用于异步处理日志内容的线程池
     */
    private static void processLogContent(InputStream logStream, ExecutorService executor) {
        StringBuilder recordBuilder = new StringBuilder();
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;

        // 使用 CharsetDecoder 处理字节流
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        //  创建 ByteBuffer 和 CharBuffer，字节容量为buffer的长度+3，+3是因为UTF-8编码最多有4个字节，不完整的UTF-8字符最多占3个字节，避免字符截断导致的乱码问题
        ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE+3);
        CharBuffer charBuffer = CharBuffer.allocate(BUFFER_SIZE+3);

        try (BufferedWriter csvWriter = new BufferedWriter(new FileWriter("test.csv"));
             BufferedWriter invalidWriter = new BufferedWriter(new FileWriter("invalid_records.txt"))) {

            Future<?> future = null;
            while ((bytesRead = logStream.read(buffer)) != -1) {
                // 将字节数据放入 ByteBuffer
                byteBuffer.put(buffer, 0, bytesRead);
                byteBuffer.flip(); // 切换到读模式

                // 解码字节为字符
                CoderResult result = decoder.decode(byteBuffer, charBuffer, false);
                if (result.isError()) {
                    throw new IOException("字符解码失败");
                }

                // 将字符数据追加到 StringBuilder
                charBuffer.flip(); // 切换到读模式
                recordBuilder.append(charBuffer);
                charBuffer.clear(); // 清空 CharBuffer

                // 检查缓冲区长度是否超过最大限制
                if (recordBuilder.length() > MAX_BUFFER_SIZE) {
                    String pattern = "(?<!\\\\);";
                    Pattern compiledPattern = Pattern.compile(pattern);
                    Matcher matcher = compiledPattern.matcher(recordBuilder);

                    if (matcher.find()) {
                        // 按记录分隔符分割日志内容
                        String[] records = recordBuilder.toString().split("(?<!\\\\);");
                        // 保留未完整处理的记录
                        recordBuilder = new StringBuilder(records[records.length - 1] + (recordBuilder.charAt(recordBuilder.length()-1) == ';' ? ";" : ""));

                        // 等待消费者线程处理完缓冲区数据
                        if (future != null) {
                            future.get();
                        }

                        // 提交消费者线程处理记录
                        future = executor.submit(() -> processRecords(records, records.length-1,csvWriter, invalidWriter));
                    } else {
                        throw new IOException("Buffer size exceeded without record separator.");
                    }
                }

                byteBuffer.compact(); // 切换到写模式，保留未处理的字节
            }

            // 等待消费者线程处理完缓冲区数据，避免还有数据没有处理完成就已经退出
            if (future != null) {
                future.get();
            }

            // 处理最后一条记录
            if (recordBuilder.length() > 0) {
                // 按记录分隔符分割日志内容
                String[] records = recordBuilder.toString().split("(?<!\\\\);");
//                future = (Future<Void>) executor.submit(() -> processRecords(records, records.length, csvWriter, invalidWriter));
//                future.get();
                //最后的数据不需要异步处理，因为数据已经获取完毕
                processRecords(records, records.length, csvWriter, invalidWriter);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (Exception e){
            throw e;
        }
    }


    /**
     * 处理记录并将其写入有效的CSV文件和无效记录文件
     *
     * @param records    待处理的记录数组
     * @param size       记录数组的大小
     * @param csvWriter  用于写入CSV文件的BufferedWriter对象
     * @param invalidWriter  用于写入无效记录文件的BufferedWriter对象
     */
    private static void processRecords(String[] records, int size,BufferedWriter csvWriter, BufferedWriter invalidWriter) {
        // 构建有效记录字符串
        StringBuilder recordBuilder = new StringBuilder();
        // 构建无效记录字符串
        StringBuilder invalidBuilder = new StringBuilder();

        // 遍历记录数组
        for (int i = 0; i < records.length && i < size; i++) {
            // 分割记录为字段, 使用正则表达式匹配字段分隔符,分隔时不省略尾部空字符串
            String[] fields = records[i].split("(?<!\\\\)\\|", -1);

            // 检查字段有效性
            // 1、字段数是否只有3个
            // 2、Excel中，一个单元格最多可以包含32,767个字符
            if (fields.length == 3 &&
                    fields[0] != null && isValidTimestamp(fields[0]) &&
                    fields[1] != null && fields[1].length() <= MAX_FIELD_LENGTH &&
                    fields[2] != null && fields[2].length() <= MAX_FIELD_LENGTH
            ) {
                // 格式化时间戳
                String timestamp = formatTimestamp(fields[0]);
                // 获取操作类型
                String operation = fields[2];
                // 构建有效记录字符串
                recordBuilder.append(timestamp).append(",\"").append(operation).append("\"\n");
            } else {
                // 无效记录：写入无效记录文件
                // 获取无效原因
                String invalidReason = getInvalidReason(fields);
                // 构建无效记录字符串
                invalidBuilder.append("原始记录: ").append(records[i]).append("\n");
                invalidBuilder.append("无效原因: ").append(invalidReason).append("\n");
                invalidBuilder.append("------------------\n"); // 分隔符
            }

            // 批量写入
            if (recordBuilder.length() >= IO_BUFFER_SIZE) {
                writeBatch(csvWriter, recordBuilder);
            }
            if (invalidBuilder.length() >= IO_BUFFER_SIZE) {
                writeBatch(invalidWriter, invalidBuilder);
            }
        }
        // 批量写入剩余数据
        if(recordBuilder.length() > 0) {
            writeBatch(csvWriter, recordBuilder);
        }
        if (invalidBuilder.length() > 0) {
            writeBatch(invalidWriter, invalidBuilder);
        }
    }

    /**
     * 检查时间戳是否有效。
     *
     * @param timestamp 时间戳
     * @return 时间戳是否有效
     */
    private static boolean isValidTimestamp(String timestamp) {
        try {
            Long.parseLong(timestamp);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }


    /**
     * 获取无效记录的原因。
     *
     * @param fields 记录字段
     * @return 无效原因
     */
    private static String getInvalidReason(String[] fields) {
        if (fields.length != 3) {
            return "字段数量不符，应为 3 个字段，实际为 " + fields.length + " 个字段";
        }
        if (fields[0] == null || !isValidTimestamp(fields[0])) {
            return "字段 1（时间戳）无效：时间戳无效";
        }
        if (fields[1] != null && fields[1].length() > MAX_FIELD_LENGTH) {
            return "字段 2（操作者）无效：长度超过 " + MAX_FIELD_LENGTH + " 个字符";
        }
        if (fields[2] != null && fields[2].length() > MAX_FIELD_LENGTH) {
            return "字段 3（操作内容）无效：长度超过 " + MAX_FIELD_LENGTH + " 个字符";
        }
        return "未知原因";
    }

    private static void writeBatch(BufferedWriter writer, StringBuilder stringBuilder) {
        try  {
            writer.write(stringBuilder.toString());
            stringBuilder.setLength(0);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 将时间戳格式化为可读的日期时间字符串。
     *
     * @param timestamp 时间戳字符串
     * @return 格式化后的日期时间字符串，如果时间戳无效则返回"Invalid Timestamp"
     */
    private static String formatTimestamp(String timestamp) {
        try {
            long timeMillis = Long.parseLong(timestamp);
            Date date = new Date(timeMillis);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return sdf.format(date);
        } catch (NumberFormatException e) {
            return "Invalid Timestamp";
        }
    }

    /**
     * 打印当前JVM的内存使用情况和剩余可用内存。
     */
    private static void printMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory(); // JVM最大可用内存
        long usedMemory = runtime.totalMemory() - runtime.freeMemory(); // 已使用内存
        long freeMemory = maxMemory - usedMemory; // 剩余可用内存

//        System.out.println("最大内存: " + maxMemory / (1024 * 1024) + " MB");
//        System.out.println("已使用内存: " + usedMemory / (1024 * 1024) + " MB");
        System.out.println("剩余可用内存: " + freeMemory / (1024 * 1024) + " MB");
    }

}
