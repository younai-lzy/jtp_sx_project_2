package com.sina;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.regex.Matcher;


public class ParseIPUtil extends UDF {
  // 用于检查IP地址格式是否正确 (简单的IPv4正则)
  private static final java.util.regex.Pattern IP_PATTERN = java.util.regex.Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

  // ip2region 数据库文件在JAR包内的路径 (resources目录下的文件名)
  private static final String IP2REGION_DB_RESOURCE_PATH = "ip2region.db";

  // DbSearcher 是 ip2region 库的核心查询对象，只需要初始化一次
  private static DbSearcher searcher = null;
  // 使用反射来调用方法，提高性能
  private static Method method = null;

  // 静态代码块：这个部分只会在UDF类第一次被加载时执行一次，用于初始化字典（DbSearcher）
  static {
    try {
      // 通过 ClassLoader 从 JAR 包内部读取资源文件
      InputStream dbInputStream = ParseIPUtil.class.getClassLoader().getResourceAsStream(IP2REGION_DB_RESOURCE_PATH);

      if (dbInputStream == null) {
        // 如果资源文件不存在，打印错误信息
        System.err.println("ip2region.db resource not found in JAR at: " + IP2REGION_DB_RESOURCE_PATH);
        throw new IOException("ip2region.db resource not found in JAR!");
      }

      // 初始化 DbSearcher，传入配置文件和输入流
      DbConfig config = new DbConfig();
      searcher = new DbSearcher(config, dbInputStream.toString()); // 使用 InputStream 初始化

      // 获取 btreeSearch 方法，用于后续查询
      method = searcher.getClass().getMethod("btreeSearch", String.class);

      System.out.println("ip2region DbSearcher initialized successfully from JAR resource: " + IP2REGION_DB_RESOURCE_PATH);

    } catch (Exception e) {
      // 如果初始化失败，打印错误信息，并把 searcher 设为 null
      System.err.println("Failed to initialize ip2region DbSearcher: " + e.getMessage());
      e.printStackTrace();
      searcher = null; // 标记为失败
    }
  }

  /**
   * 检查IP地址的每个八位字节是否在有效范围内 (0-255)。
   * @param ip IP地址字符串
   * @return 如果所有八位字节都有效则返回 true，否则返回 false。
   */
  private boolean isValidOctetRange(String ip) {
    String[] octets = ip.split("\\.");
    if (octets.length != 4) {
      return false; // 不是IPv4的四个八位字节
    }
    for (String octet : octets) {
      try {
        int value = Integer.parseInt(octet);
        if (value < 0 || value > 255) {
          return false; // 八位字节超出范围
        }
      } catch (NumberFormatException e) {
        return false; // 无法解析为数字
      }
    }
    return true;
  }

  /**
   * evaluate 方法是UDF的核心逻辑。Hive会为每一行数据调用这个方法。
   *
   * @param ipAddress 需要解析的IP地址字符串 (例如: "117.127.228.52")。
   * @return 一个 Text 对象，包含解析后的地理位置字符串 (例如: "中国,江苏省,南京市")
   * 或者错误/提示信息。
   */
  public Text evaluate(Text ipAddress) {
    // 1. 处理空输入或空白输入
    if (ipAddress == null || ipAddress.toString().trim().isEmpty()) {
      return new Text("Invalid IP: Input is null or empty");
    }

    String ip = ipAddress.toString().trim();

    // 2. 验证IP地址格式是否正确 (使用正则和八位字节范围验证)
    Matcher matcher = IP_PATTERN.matcher(ip);
    if (!matcher.matches() || !isValidOctetRange(ip)) {
      return new Text("Invalid IP: Format mismatch or invalid IP address");
    }

    // 3. 执行IP地址查询
    if (searcher == null || method == null) {
      // 如果初始化失败，直接返回错误
      return new Text("Error: ip2region searcher not initialized. Check UDF logs.");
    }

    try {
      // 通过反射调用 btreeSearch 方法进行查询
      DataBlock dataBlock = (DataBlock) method.invoke(searcher, ip);
      if (dataBlock != null) {
        // ip2region 返回的结果格式通常是: 国家|区域|省份|城市|ISP
        // 例如: 中国|0|江苏省|南京市|电信
        String region = dataBlock.getRegion();
        String[] parts = region.split("\\|"); // 按 | 分割字符串

        String country = "Unknown";
        String province = "Unknown";
        String city = "Unknown";

        // 根据分割后的部分提取国家、省份、城市
        if (parts.length >= 1 && !parts[0].equals("0")) {
          country = parts[0];
        }
        // parts[1] 通常是 "0" 代表国内区域，我们跳过
        if (parts.length >= 3 && !parts[2].equals("0")) {
          province = parts[2];
        }
        if (parts.length >= 4 && !parts[3].equals("0")) {
          city = parts[3];
        }

        // 拼接成 "国家,省份,城市" 的格式返回
        return new Text(country + "," + province + "," + city);
      } else {
        // 如果没有查到结果
        return new Text("Location Not Found for IP: " + ip);
      }
    } catch (Exception e) {
      // 查询过程中发生错误
      System.err.println("Error during ip2region lookup for IP " + ip + ": " + e.getMessage());
      e.printStackTrace();
      return new Text("Error during lookup: " + e.getMessage());
    }
  }

  // close 方法：在UDF不再需要时关闭资源，防止内存泄露。
  public void close() throws IOException {
    if (searcher != null) {
      searcher.close();
    }
  }
}
