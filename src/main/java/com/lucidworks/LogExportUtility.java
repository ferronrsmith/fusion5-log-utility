package com.lucidworks;

import com.jayway.jsonpath.JsonPath;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.File;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LogExportUtility {

  @Option(name = "-exportTo", usage = "The path of the directory to export to.", required = true)
  private String exportTo;
  @Option(name = "-username", usage = "The username of which to authenticate. Either specify username and password, or you can specify cookieStr.")
  private String username;
  @Option(name = "-password", usage = "The password of which to authenticate. Either specify username and password, or you can specify cookieStr.")
  private String password;
  @Option(name = "-cookieStr", usage = "The session cookie from Fusion api-gateway. You can specify cookieStr, or alternatively you can specify username/password.")
  private String cookieStr;
  @Option(name = "-fusionUrl", usage = "The Fusion URL", required = true)
  private String fusionUrl;
  @Option(name = "-from", usage = "'From' timestamp in solr timestamp format. Example: 2022-03-21T14:31:30.000Z", required = true)
  private String from;
  @Option(name = "-numRowsPerPage", usage = "Optional - The number of rows per page.")
  private int numRowsPerPage = 5000;
  @Option(name = "-to", usage = "Optional - defaults to *. 'To' timestamp in solr timestamp format. Example: 2022-03-21T14:31:30.000Z")
  private String to = "*";
  @Option(name = "-q", usage = "Optional - Filter on the logs to get. By default *:*")
  private String q = "*:*";

  public static void main(String[] args) throws Exception {
    LogExportUtility logExportUtility = new LogExportUtility();
    CmdLineParser parser = new CmdLineParser(logExportUtility);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      parser.printUsage(System.out);
      return;
    }
    logExportUtility.run();
  }

  public void run() throws Exception {

    File outDir = new File(exportTo, new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSZZZZ").format(new Date()));
    outDir.mkdirs();

    BasicCookieStore cookieStore = new BasicCookieStore();
    SSLConnectionSocketFactory sslsf;

    if (StringUtils.isBlank(cookieStr)) {
      HttpPost post = new HttpPost(fusionUrl + "/api/session");
      post.setEntity(new StringEntity("{\"username\":\"" + username + "\", \"password\":\"" + password + "\"}"));
      post.setHeader("Content-Type", "application/json");
      try (CloseableHttpClient client = HttpClients.custom()
          .setDefaultCookieStore(cookieStore)
          .build();
           CloseableHttpResponse resp = client.execute(post)) {
        if (resp.getStatusLine().getStatusCode() < 200 || resp.getStatusLine().getStatusCode() > 299) {
          throw new Exception("Auth failed to get session. Status code: " + resp.getStatusLine());
        }
        Header[] headers = resp.getHeaders("Set-Cookie");
        for (Header h : headers) {
          cookieStr = h.getValue().split("=")[1];
          break;
        }
        System.out.println("Authenticated.");
      }
    }
    BasicClientCookie cookie = new BasicClientCookie("id", cookieStr);
    cookie.setDomain(new URL(fusionUrl).getHost());
    cookieStore.addCookie(cookie);

    try {
      SSLContextBuilder sslContextBuilder = SSLContexts.custom();
      sslContextBuilder.loadTrustMaterial(null, (TrustStrategy) (chain, authType) -> true);
      SSLContext sslContext = sslContextBuilder.build();
      sslsf = new SSLConnectionSocketFactory(
          sslContext, new X509HostnameVerifier() {
        @Override
        public void verify(String host, SSLSocket ssl) {
        }

        @Override
        public void verify(String host, X509Certificate cert) {
        }

        @Override
        public void verify(String host, String[] cns, String[] subjectAlts) {
        }

        @Override
        public boolean verify(String s, SSLSession sslSession) {
          return true;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException("Could not ignore SSL verification", e);
    }

    int curIndex = 0;
    List<Map> data = null;
    HttpClientBuilder httpClientBuilder = HttpClients.custom()
        .setDefaultCookieStore(cookieStore)
        .setSSLSocketFactory(sslsf);
    try (CloseableHttpClient client = httpClientBuilder
        .build()) {
      while (data == null || data.size() > 1) {
        System.out.println("On Record Count " + curIndex);
        String uri =
            fusionUrl + "/api/solr/system_logs/query?wt=json&sort=timestamp_tdt%20asc&q=" +
                URLEncoder.encode(q, "UTF-8") + "&rows=" + numRowsPerPage +
                "&start=" + curIndex +
                "&fq=timestamp_tdt:[" + from + "%20TO%20" + to + "]&fq=type_s:java";
        System.out.println("next query: " + uri);
        HttpGet request = new HttpGet(
            uri);
        request.setHeader("Content-Type", "application/json;charset=utf-8");
        try (CloseableHttpResponse response = client.execute(request)) {
          if (response.getStatusLine().getStatusCode() > 299 || response.getStatusLine().getStatusCode() < 200) {
            throw new Exception("Unexpected status code " + response.getStatusLine());
          }
          data = JsonPath.read(response.getEntity().getContent(), "response.docs");
          for (Map dataMap : data) {
            String service_s = (String) dataMap.get("service_s");
            String kubernetes_pod_name_s = (String) dataMap.get("kubernetes_pod_name_s");
            String level_s = (String) dataMap.get("level_s");
            String timestamp_tdt = (String) dataMap.get("timestamp_tdt");
            String logger_class_s = (String) dataMap.get("logger_class_s");
            String message_txt = "";
            if (dataMap.get("message_txt") != null) {
              message_txt = dataMap.get("message_txt") instanceof String ? (String) dataMap.get("message_txt") : String.valueOf(((List) dataMap.get("message_txt")).get(0));
            }
            String thread_s = (String) dataMap.get("thread_s");
            String stack_trace_txt = "";
            if (dataMap.get("stack_trace_txt") != null) {
              stack_trace_txt = " " + (dataMap.get("stack_trace_txt") instanceof String ? (String) dataMap.get("stack_trace_txt") : String.valueOf(((List) dataMap.get("stack_trace_txt")).get(0)));
            }

            String caller_line_number_s = (String) dataMap.get("caller_line_number_s");

            String filename = service_s + "_" + kubernetes_pod_name_s + ".log";
            FileUtils.writeStringToFile(new File(outDir, filename),
                String.format("%s - %s [%s:%s@%s] - %s%s%n", timestamp_tdt, level_s, thread_s, logger_class_s,
                    caller_line_number_s, message_txt, stack_trace_txt), StandardCharsets.UTF_8, true);
          }
        }
        curIndex += numRowsPerPage;
      }
    }
    System.out.println("Logs have been exported to: " + outDir);

    File outputZipFile = new File(outDir.getParentFile(), outDir.getName() + ".zip");
    System.out.println("Zipping result to: " + outputZipFile);
    ZipFile zipfile = new ZipFile(outputZipFile);
    zipfile.addFolder(outDir);
    zipfile.close();
    System.out.println("Done. Zip successfully written to " + outputZipFile);

  }
}
