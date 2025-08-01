package com.sina;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.lionsoul.ip2region.xdb.Searcher;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;



public class ParseIPUtil extends UDF {
  // 使用 volatile 关键字保证多线程可见性
  private static volatile Searcher xdbSearcherInstance; // 注意：现在是 org.lionsoul.ip2region.xdb.Searcher
  // IP2Region .xdb 数据库文件路径
  private static final String XDB_PATH = "/usr/local/ip2region/ip2region.xdb"; // 路径改为 .xdb

  // 用于确保 Searcher 只初始化一次的锁对象，保证线程安全
  private static final Object lock = new Object();

  /**
   * 私有静态方法，用于获取 Searcher 的单例实例。
   * 采用双重检查锁定（Double-Checked Locking）模式，确保线程安全和高性能。
   *
   * @return Searcher 的单例实例
   * @throws IOException 如果数据库文件不存在或初始化失败
   */
  private static Searcher getXdbSearcherInstance() throws IOException {
    if (xdbSearcherInstance == null) {
      synchronized (lock) {
        if (xdbSearcherInstance == null) {
          File xdbFile = new File(XDB_PATH);
          if (!xdbFile.exists()) {
            System.err.println("ERROR: IP2Region .xdb database file NOT FOUND at: " + XDB_PATH);
            throw new IOException("IP2Region .xdb 数据库文件未找到，路径：" + XDB_PATH);
          }

          // For .xdb, it's recommended to load the entire file into memory for best performance
          // This is done once due to the singleton pattern.
          byte[] cBuff;
          try {
            cBuff = Files.readAllBytes(Paths.get(XDB_PATH));
          } catch (IOException e) {
            System.err.println("ERROR: Failed to load IP2Region .xdb file into memory from " + XDB_PATH + ": " + e.getMessage());
            e.printStackTrace();
            throw new IOException("加载 IP2Region .xdb 文件到内存失败：" + e.getMessage(), e);
          }

          try {
            // 使用新的 Searcher.newWithBuffer 方法初始化
            xdbSearcherInstance = Searcher.newWithBuffer(cBuff);
            System.out.println("INFO: IP2Region .xdb Searcher initialized successfully from: " + XDB_PATH);
          } catch (Exception e) {
            System.err.println("ERROR: Failed to initialize IP2Region .xdb Searcher from " + XDB_PATH + ": " + e.getMessage());
            e.printStackTrace();
            throw new IOException("初始化 IP2Region .xdb Searcher 失败：" + e.getMessage(), e);
          }
        }
      }
    }
    return xdbSearcherInstance;
  }

  /**
   * UDF 的构造函数。
   */
  public ParseIPUtil() {
    // 构造函数保持简单
  }

  /**
   * 评估方法：解析给定的 IP 地址，并返回一个包含地域信息的 Map。
   *
   * @param str 要解析的 IP 地址字符串。
   * @return 一个 Map，包含解析后的国家（country）、区域（area）、省份（province）、
   * 城市（city）和 ISP（isp）信息。
   * @throws Exception 如果在 IP 查找或解析过程中发生错误。
   */
  public Map<String, String> evaluate(String str) throws Exception {
    Map<String, String> map = new HashMap<>();

    // 获取 Searcher 的单例实例
    Searcher currentSearcher = getXdbSearcherInstance();

    // 调用 Searcher 进行查找
    // 注意：xdb Searcher 的 search 方法直接返回字符串
    String region = currentSearcher.search(str);

    // 解析地域字符串
    String[] split = region.split("\\|");
    // .xdb 格式返回的字符串与 .db 格式略有不同，但通常仍是 "国家|区域|省份|城市|ISP"
    // 确保你的 ip2region.xdb 文件是完整的，并且返回格式符合预期
    if (split.length >= 5) {
      map.put("country", split[0]);
      map.put("area", split[1]);
      map.put("province", split[2]);
      map.put("city", split[3]);
      map.put("isp", split[4]);
    } else {
      // 处理地域字符串格式不符合预期的情况
      map.put("country", split.length > 0 ? split[0] : "");
      map.put("area", split.length > 1 ? split[1] : "");
      map.put("province", split.length > 2 ? split[2] : "");
      map.put("city", split.length > 3 ? split[3] : "");
      map.put("isp", split.length > 4 ? split[4] : "");
      System.err.println("Warning: IP2Region .xdb returned an unexpected format for IP: " + str + ", region: " + region);
    }

    return map;
  }
}
