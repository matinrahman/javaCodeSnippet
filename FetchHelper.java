package com.matin.flms.helper;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.*;
import com.gargoylesoftware.htmlunit.javascript.background.JavaScriptJobManager;
import com.github.junrar.Archive;
import com.github.junrar.exception.RarException;
import com.github.junrar.rarfile.FileHeader;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.mysql.jdbc.StringUtils;
import com.ra.flms.Common;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

//import jj2000.j2k.codestream.HeaderInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;


public class FetchHelper {

  private static WebClient webClient = new WebClient(BrowserVersion.FIREFOX_52);
  private static HashMap<String, DomNode> contextMap = new HashMap<>();
  private static String lastDownloadedFileName = "";// used in the case if the URL throws 404
  private static boolean skipLength = false;
  private static boolean  loopTrackingFlag=true;
  private static int countMessageFileCounter=0;
  private static int targetMessageFileCounter=0;


  static {
    /*
     * Create new WebCLient
     */
    webClient = new WebClient(BrowserVersion.FIREFOX_52);
    webClient.getOptions().setJavaScriptEnabled(true);
    webClient.getOptions().setThrowExceptionOnScriptError(false);
    webClient.getOptions().setCssEnabled(false);
    webClient.setAjaxController(new NicelyResynchronizingAjaxController());
    webClient.getOptions().setUseInsecureSSL(true);
    webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
    webClient.getCookieManager().setCookiesEnabled(true);
  }

  public static TrustManager[] trustAllCerts = new TrustManager[]{
    new X509TrustManager() {
      @Override
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      @Override
      public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
        //No need to implement.
      }

      @Override
      public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
        //No need to implement.
      }
    }
  };

  public static HostnameVerifier hv = new HostnameVerifier() {
    @Override
    public boolean verify(String urlHostName, SSLSession session) {
      return true;
    }
  };

  public static List<String> extractFilesFromZip(String outputDirectory, String zipFilePath, List<String> excludedEntensions, boolean override) {
    List<String> extractedFiles = new ArrayList<>();
    int countExtract=0;

    if (new File(zipFilePath).isFile() && new File(zipFilePath).length() > 0) {
      try {
        File folder = new File(outputDirectory);
        if (!folder.exists()) {
          folder.mkdirs();
        }

        if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
          Common.dbg.println("Opening: " + zipFilePath);
        } else {
          System.out.println("Opening: " + zipFilePath);
        }

        ZipFile zipFile = null;
        ZipInputStream zis = null;
        boolean useZis = false;

        try {
          zipFile = new ZipFile(zipFilePath);
        } catch (IOException e) {
          if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
            Common.dbg.println("Trying alternate method to extract...");
          } else {
            System.out.println("Trying alternate method to extract...");
          }
          zis = new ZipInputStream(new WinZipInputStream(new FileInputStream(zipFilePath)));
          useZis = true;
        }

        if (useZis && zis != null) {
          ZipEntry ze = zis.getNextEntry();
          while (ze != null) {
            String fileFromZip = ze.getName();
            if (ze.isDirectory()) {
              continue;
            } else {
              if (fileFromZip.contains("\\")) {
                fileFromZip = fileFromZip.substring(fileFromZip.lastIndexOf("\\") + 1);
              }
              if (fileFromZip.contains("/")) {
                fileFromZip = fileFromZip.substring(fileFromZip.lastIndexOf("/") + 1);
              }
            }

            String extension = "";
            if (fileFromZip.contains(".")) {
              extension = fileFromZip.substring(fileFromZip.lastIndexOf(".")).toUpperCase();
            }

            if (!excludedEntensions.contains(extension)) {
              System.out.println("EXTRACTING: " + outputDirectory.concat(File.separator).concat(fileFromZip));

              File newFile = new File(outputDirectory.concat(File.separator), fileFromZip);
              File bakFile = new File(outputDirectory.concat(File.separator), fileFromZip + ".bak");
              if (newFile.exists()) {
                Files.move(newFile.toPath(), bakFile.toPath(), REPLACE_EXISTING);
                bakFile = new File(outputDirectory.concat(File.separator), fileFromZip + ".bak");
              }

              FileOutputStream fos = new FileOutputStream(newFile);
              BufferedOutputStream bos = new BufferedOutputStream(fos, 1024);

              writeFile(zis, bos);

              bos.flush();
              bos.close();

              if (override) {
                extractedFiles.add(fileFromZip);
              } else if (!FileUtils.contentEquals(newFile, bakFile)) {
                extractedFiles.add(fileFromZip);
              } else {
                System.out.println("SKIP. SAME AS OLD FILE - ".concat(fileFromZip));
              }
              if (bakFile.exists()) {
                bakFile.delete();
              }

            }
            ze = zis.getNextEntry();

          }
          zis.closeEntry();
          zis.close();
        } else if (zipFile != null) {
          Enumeration e = zipFile.entries();
          while (e.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) e.nextElement();
            String fileFromZip = entry.getName();

            if (entry.isDirectory()) {
              continue;
            } else {
              if (fileFromZip.contains("\\")) {
                fileFromZip = fileFromZip.substring(fileFromZip.lastIndexOf("\\") + 1);
              }
              if (fileFromZip.contains("/")) {
                fileFromZip = fileFromZip.substring(fileFromZip.lastIndexOf("/") + 1);
              }
            }

            String extension = "";
            if (fileFromZip.contains(".")) {
              extension = fileFromZip.substring(fileFromZip.lastIndexOf(".")).toUpperCase();
            }

            if (!excludedEntensions.contains(extension)) {
              System.out.println("EXTRACTING: " + outputDirectory.concat(File.separator).concat(fileFromZip));
              File newFile = new File(outputDirectory.concat(File.separator), fileFromZip);
              File bakFile = new File(outputDirectory.concat(File.separator), fileFromZip + ".bak");
              if (newFile.exists()) {
                Files.move(newFile.toPath(), bakFile.toPath(), REPLACE_EXISTING);
                bakFile = new File(outputDirectory.concat(File.separator), fileFromZip + ".bak");
              }

              FileOutputStream fos = new FileOutputStream(newFile);
              BufferedOutputStream bos = new BufferedOutputStream(fos, 1024);
              BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(entry));

              writeFile(bis, bos);

              bis.close();
              bos.flush();
              bos.close();
              if (override) {
                extractedFiles.add(fileFromZip);
              } else if (!FileUtils.contentEquals(newFile, bakFile)) {
                extractedFiles.add(fileFromZip);
              } else {
                System.out.println("SKIP. SAME AS OLD FILE - ".concat(fileFromZip));
              }
              if (bakFile.exists()) {
                bakFile.delete();
              }
            }
          }
        }
      } catch (IOException ex) {
        Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
    return extractedFiles;
  }

  public static List<String> extractFilesFromRar(String outputDirectory, String fileName, List<String> excludedEntensions, boolean override) {
    List<String> extractedFiles = new ArrayList<>();
    try {
      File folder = new File(outputDirectory);
      if (!folder.exists()) {
        folder.mkdir();
      }
      Archive a = new Archive(new File(fileName));
      if (a != null) {
        FileHeader fh = a.nextFileHeader();
        while (fh != null) {
          try {
            if (!fh.isDirectory()) {
              String fileFromRar;
              if (fh.isUnicode()) {
                fileFromRar = fh.getFileNameW().trim();
              } else {
                fileFromRar = fh.getFileNameString().trim();
              }
              if (fileFromRar.indexOf("/") > 0) {
                fileFromRar = fileFromRar.substring(fileFromRar.lastIndexOf("/") + 1);
              }
              if (fileFromRar.indexOf("\\") > 0) {
                fileFromRar = fileFromRar.substring(fileFromRar.lastIndexOf("\\") + 1);
              }
              String extension = "";
              if (fileFromRar.contains(".")) {
                extension = fileFromRar.substring(fileFromRar.lastIndexOf(".")).toUpperCase();
              }
              if (!excludedEntensions.contains(extension)) {
                System.out.println("EXTRACTING: " + outputDirectory.concat(File.separator).concat(fileFromRar));
                File newFile = new File(outputDirectory.concat(File.separator), fileFromRar);
                File bakFile = new File(outputDirectory.concat(File.separator), fileFromRar + ".bak");
                int b;
                if (newFile.exists()) {
                  Files.move(newFile.toPath(), bakFile.toPath(), REPLACE_EXISTING);
                  bakFile = new File(outputDirectory.concat(File.separator), fileFromRar + ".bak");
                }
                FileOutputStream fos = new FileOutputStream(newFile);
                InputStream is = a.getInputStream(fh);

                writeFile(is, fos);

                is.close();
                fos.flush();
                fos.close();

                if (override) {
                  extractedFiles.add(fileFromRar);
                } else if (!FileUtils.contentEquals(newFile, bakFile)) {
                  extractedFiles.add(fileFromRar);
                } else {
                  System.out.println("SKIP. SAME AS OLD FILE - ".concat(fileFromRar));
                }
                if (bakFile.exists()) {
                  bakFile.delete();
                }
              }
            }
          } catch (FileNotFoundException ex) {
            Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
          } catch (RarException | IOException ex) {
            Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
          }
          fh = a.nextFileHeader();
        }
      }
    } catch (RarException | IOException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    return extractedFiles;
  }

  public static List<String> downloadListFTP(FTPClient ftpClient, Map<String, String> fileList) {
    List<String> modified = new ArrayList<>();
	int countDownload = 0;
	Common.startMessage("Fetch");
	Common.sizeMessage(1);
	int totalTarget=fileList.size();
	Common.targetMessage(fileList.size());
    for (String location : fileList.keySet()) {
      String filePath = fileList.get(location);
      boolean isDifferent = true;
      File file = new File(filePath);
      File bakFile = new File(filePath.concat(".bak"));

      if (file.exists()) {
        isDifferent = isDifferentFromFTP(ftpClient, file, location);
      }

      if (isDifferent) {
        if (bakFile.exists()) {
          bakFile.delete();
        }
        if (file.exists()) {
          try {
            Files.move(file.toPath(), bakFile.toPath(), REPLACE_EXISTING);
          } catch (IOException ex) {
            Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException ex) {
          Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (downloadFileFTP(ftpClient, location, fileList.get(location))) {
          modified.add(filePath);
        }
      }
	  countDownload++;
      Common.countMessage(countDownload);
    }
    Common.countMessage(totalTarget);
	Common.endMessage("Fetch");
    return modified;
  }

  public static boolean downloadFileFTP(FTPClient ftpClient, String remoteFilePath, String savePath) {
    File downloadFile = new File(savePath);
    File parentDir = downloadFile.getParentFile();
    if (!parentDir.exists()) {
      parentDir.mkdirs();
    }

    try {
      ftpClient.setKeepAlive(true);
      OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
      ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

      InputStream inputStream = ftpClient.retrieveFileStream(remoteFilePath);

      writeFile(inputStream, outputStream);

      boolean success = ftpClient.completePendingCommand();
      outputStream.close();
      inputStream.close();
      if (success) {
        long remoteLength = 0;
        FTPFile[] files = ftpClient.listFiles(remoteFilePath);
        if (files.length == 1 && files[0].isFile()) {
          remoteLength = files[0].getSize();
        }

        if (downloadFile.length() != remoteLength) {
          System.out.println("SIZE MISMATCH: Reported - " + remoteLength + ", Downloaded - " + downloadFile.length());
          return false;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String time = ftpClient.getModificationTime(remoteFilePath).contains(" ") ? ftpClient.getModificationTime(remoteFilePath).split(" ")[1] : ftpClient.getModificationTime(remoteFilePath);
        long remoteModified = sdf.parse(time).getTime();
        if (downloadFile.setLastModified(remoteModified)) {
          return true;
        } else {
          System.out.println("Erorr modifying Last Modified Time");
        }
      }

    } catch (IOException | ParseException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    return false;
  }

  public static List<String> downloadListURL(Map<String, String> fileList) {
    int countDownload=0;
    List<String> modified = new ArrayList<>();
	Common.startMessage("Fetch");
	Common.sizeMessage(1);
	int totalTarget=fileList.size();
	Common.targetMessage(fileList.size());
    for (String location : fileList.keySet()) {
      String filePath = fileList.get(location);
      boolean isDifferent = true;
      File file = new File(filePath);
      File bakFile = new File(filePath.concat(".bak"));

      if (file.exists()) {
        isDifferent = isDifferentFromURL(file, location);
      }

      if (isDifferent) {
        if (bakFile.exists()) {
          bakFile.delete();
        }
        if (file.exists()) {
          try {
            Files.move(file.toPath(), bakFile.toPath(), REPLACE_EXISTING);
          } catch (IOException ex) {
            Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException ex) {
          Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
        }


        if (downloadFileURL(file, location, false)) {
          modified.add(filePath);
          countDownload++;
          Common.countMessage(countDownload);
        }
        
      }
    }
    Common.countMessage(totalTarget);
	Common.endMessage("Fetch");
    return modified;
  }

  private static Boolean downloadDropBox(String location, File file) {

    InputStream is = null;
    BufferedOutputStream outputStream = null;
    Boolean ret = true;
    try {
        URL fileUrl = new URL(location);
        HttpURLConnection connection = (HttpURLConnection)fileUrl.openConnection();
        connection.connect();
        is = connection.getInputStream();

        BufferedInputStream inputStream = new BufferedInputStream(is);
        file.getParentFile().mkdirs();
        OutputStream os = new FileOutputStream(file);
        outputStream = new BufferedOutputStream(os);

        writeFile(inputStream, outputStream);

        outputStream.close();
        inputStream.close();

    } catch (Exception e) {
      e.printStackTrace();
      ret = false;
    }
    finally {
        if (is != null) {
            try {
                is.close();

            } catch (IOException e) {
                e.printStackTrace();
                ret = false;
            }
        }
        if (outputStream != null) {
            try {
                outputStream.flush();
                outputStream.close();

            } catch (IOException e) {
                e.printStackTrace();
                ret = false;
            }
        }
    }
    return  ret;
  }

  public static List<String> scrapeURL(Map<String, String> fileList, String downloadUrl, String xPath, String fileXPath, Map<String, String> replaceURLMap, boolean skipContentLengthFromURL, Map<String, String> multipleFileXPaths, Integer year) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE);
    java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit.javascript.StrictErrorReporter").setLevel(java.util.logging.Level.OFF);
    webClient.getOptions().setJavaScriptEnabled(true);
    webClient.getOptions().setThrowExceptionOnScriptError(false);
    webClient.getOptions().setCssEnabled(false);
    webClient.setAjaxController(new NicelyResynchronizingAjaxController());
    webClient.getOptions().setUseInsecureSSL(true);
    webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
    webClient.getCookieManager().setCookiesEnabled(true);

    Object[] enclosure = new Object[2];
    List dnl = null;
    try {
      DomNode context = webClient.getPage(downloadUrl);
      if (skipContentLengthFromURL) {
        HtmlPage p = (HtmlPage) context;
        p.initialize();
        webClient.waitForBackgroundJavaScript(10000);
      }
      dnl = context.getByXPath(xPath);
    } catch (IOException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    } catch (FailingHttpStatusCodeException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }

    List<String> modified = new ArrayList<>();
    for (String location : fileList.keySet()) {
      String filePath = fileList.get(location);
      String ext = filePath.contains(".") ? filePath.substring(filePath.lastIndexOf(".") + 1) : "";
      if (StringUtils.isEmptyOrWhitespaceOnly(ext)) {
        filePath = filePath.concat(".zip");
      }
      boolean isDifferent = true;
      File file = new File(filePath);
      File bakFile = new File(filePath.concat(".bak"));
      String downloadURl = "";
      for (Object obj : dnl) {
        HtmlElement e = (HtmlElement) obj;
        if (e.asXml().contains(location)) {
          //System.out.println("multipleFileXPaths.get(location) : " + multipleFileXPaths.get(location));
          if (multipleFileXPaths.get(location) != null) {
            List fileAnchor = e.getByXPath(multipleFileXPaths.get(location));
            if (fileAnchor != null && fileAnchor.size() > 0) {
              HtmlAnchor anchorTag = (HtmlAnchor) fileAnchor.get(0);
              downloadURl = anchorTag.getHrefAttribute();
              if (replaceURLMap.size() > 0) {
                for (String replaceString : replaceURLMap.keySet()) {
                  downloadURl = downloadURl.replace(replaceString, replaceURLMap.get(replaceString));
                }
              }
              if (downloadURl.startsWith("/")) {
                if(com.ra.flms.cad.fetch.Common.baseDownloadUrl!="")
                  downloadURl = com.ra.flms.cad.fetch.Common.baseDownloadUrl.concat(downloadURl);
                else
                downloadURl = com.ra.flms.cad.fetch.Common.downloadUrl.concat(downloadURl);
              }
            }
            break;
          }
        }
      }
      if (!StringUtils.isEmptyOrWhitespaceOnly(downloadURl) && file.exists()) {
        isDifferent = isDifferentFromURL(file, downloadURl);
      }

      if (isDifferent) {
        if (bakFile.exists()) {
          bakFile.delete();
        }
        if (file.exists()) {
          try {
            Files.move(file.toPath(), bakFile.toPath(), REPLACE_EXISTING);
          } catch (IOException ex) {
            Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException ex) {
          Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (!StringUtils.isEmptyOrWhitespaceOnly(downloadURl) && downloadFileURL(file, downloadURl, skipContentLengthFromURL)) {
          modified.add(filePath);
        }
      }
    }
    return modified;
  }

  public static List<String> scrapeURLWithLogin(Map<String, String> fileList, String downloadUrl, String xPath, String fileXPath, Map<String, String> replaceURLMap, boolean skipContentLengthFromURL, Map<String, String> multipleFileXPaths, Integer year, String username, String password, String loginUrl) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE);
    java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit.javascript.StrictErrorReporter").setLevel(java.util.logging.Level.OFF);
    webClient.getOptions().setJavaScriptEnabled(true);
    webClient.getOptions().setThrowExceptionOnScriptError(false);
    webClient.getOptions().setCssEnabled(false);
    webClient.setAjaxController(new NicelyResynchronizingAjaxController());
    webClient.getOptions().setUseInsecureSSL(true);
    webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
    webClient.getCookieManager().setCookiesEnabled(true);

    Object[] enclosure = new Object[2];
    List dnl = null;
    try {

      final HtmlPage loginPage = webClient.getPage(loginUrl);
      final HtmlForm form = loginPage.getFirstByXPath("//form[@id='login-form']");
      final HtmlButton login = form.getButtonByName("Submit");
      final HtmlTextInput userInput  = form.getInputByName("username");
      final HtmlPasswordInput passwordInput  = form.getInputByName("password");

      userInput.setValueAttribute(username);
      passwordInput.setValueAttribute(password);
      final HtmlPage pageAfterLogin = login.click();
      System.out.println(downloadUrl);
      HtmlPage context = null;
      try {
        context = webClient.getPage(downloadUrl);
        context.wait(10000);
      } catch (Exception e) {
        System.out.println("Get page error");
      }
      JavaScriptJobManager manager = context.getEnclosingWindow().getJobManager();
      while (manager.getJobCount() > 0) {
        try {
          Thread.sleep(10000);
        }catch (Exception e){}

      }
      System.out.println(context.asXml());

      if (skipContentLengthFromURL) {
        HtmlPage p = (HtmlPage) context;
        p.initialize();
        webClient.waitForBackgroundJavaScript(10000);
      }
      System.out.println(context.asXml());
      dnl = context.getByXPath(xPath);
    } catch (IOException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    } catch (FailingHttpStatusCodeException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    System.out.println(dnl);
    List<String> modified = new ArrayList<>();
    for (String location : fileList.keySet()) {
      String filePath = fileList.get(location);
      String ext = filePath.contains(".") ? filePath.substring(filePath.lastIndexOf(".") + 1) : "";
      if (StringUtils.isEmptyOrWhitespaceOnly(ext)) {
        filePath = filePath.concat(".zip");
      }
      boolean isDifferent = true;
      File file = new File(filePath);
      File bakFile = new File(filePath.concat(".bak"));
      String downloadURl = "";
      for (Object obj : dnl) {
        HtmlElement e = (HtmlElement) obj;
        if (e.asXml().contains(location)) {
          System.out.println("multipleFileXPaths.get(location) : " + multipleFileXPaths.get(location));
          if (multipleFileXPaths.get(location) != null) {
            List fileAnchor = e.getByXPath(multipleFileXPaths.get(location));
            if (fileAnchor != null && fileAnchor.size() > 0) {
              HtmlAnchor anchorTag = (HtmlAnchor) fileAnchor.get(0);
              downloadURl = anchorTag.getHrefAttribute();
              if (replaceURLMap.size() > 0) {
                for (String replaceString : replaceURLMap.keySet()) {
                  downloadURl = downloadURl.replace(replaceString, replaceURLMap.get(replaceString));
                }
                System.out.println(downloadURl);
              }
              if (downloadURl.startsWith("/")) {
                if(com.ra.flms.cad.fetch.Common.baseDownloadUrl!="")
                  downloadURl = com.ra.flms.cad.fetch.Common.baseDownloadUrl.concat(downloadURl);
                else
                  downloadURl = com.ra.flms.cad.fetch.Common.downloadUrl.concat(downloadURl);
              }
            }
            break;
          }
        }
      }
      if (!StringUtils.isEmptyOrWhitespaceOnly(downloadURl) && file.exists()) {
        isDifferent = isDifferentFromURL(file, downloadURl);
      }

      if (isDifferent) {
        if (bakFile.exists()) {
          bakFile.delete();
        }
        if (file.exists()) {
          try {
            Files.move(file.toPath(), bakFile.toPath(), REPLACE_EXISTING);
          } catch (IOException ex) {
            Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException ex) {
          Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (!StringUtils.isEmptyOrWhitespaceOnly(downloadURl) && downloadFileURL(file, downloadURl, skipContentLengthFromURL)) {
          modified.add(filePath);
        }
      }
    }
    return modified;
  }

//  public static boolean downloadFileURL(File file, String downloadUrl, boolean skipContentLength) {
//    try {
//
//      skipLength = skipContentLength;
//      SSLContext sc = SSLContext.getInstance("SSL");
//      sc.init(null, trustAllCerts, new java.security.SecureRandom());
//      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
//      HttpsURLConnection.setDefaultHostnameVerifier(hv);
//
//      downloadUrl = downloadUrl.replaceAll(" ", "%20");
//      boolean secure = false;
//      if (downloadUrl.contains("https")) {
//        secure = true;
//      }
//
//      SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
//      long remoteLength = 0;
//      long remoteModified = 0;
//      BufferedInputStream inputStream = null;
//      FileOutputStream outputStream = new FileOutputStream(file);
//
//      URL url = new URL(downloadUrl);
//      HttpsURLConnection cons = null;
//      HttpURLConnection con = null;
//      int status;
//      boolean compareModifiedTime = false;
//      Map<String, List<String>> headerFields;
//      if (secure) {
//        cons = (HttpsURLConnection) url.openConnection();
//        status = cons.getResponseCode();
//        headerFields = cons.getHeaderFields();
//      } else {
//        con = (HttpURLConnection) url.openConnection();
//        status = con.getResponseCode();
//        headerFields = con.getHeaderFields();
//      }
//
//      String fileBName = getFileBasename(downloadUrl);
//      if (!lastDownloadedFileName.equals(fileBName) && status == HttpURLConnection.HTTP_NOT_FOUND) {
//        String ext = getExtension(downloadUrl);
//        lastDownloadedFileName = fileBName;
//        url = new URL(downloadUrl.replace(".".concat(ext), ".".concat(ext.toUpperCase())));
//        if (secure) {
//          cons = (HttpsURLConnection) url.openConnection();
//          status = cons.getResponseCode();
//          headerFields = cons.getHeaderFields();
//        } else {
//          con = (HttpURLConnection) url.openConnection();
//          status = con.getResponseCode();
//          headerFields = con.getHeaderFields();
//        }
//      }
//
//      if (headerFields.containsKey("Last-Modified")) {
//        compareModifiedTime = true;
//      }
//
//      if (status == HttpURLConnection.HTTP_OK) {
//        if (!headerFields.containsKey("Content-Type") || (headerFields.containsKey("Content-Type") && !headerFields.get("Content-Type").contains("text/html; charset=utf-8"))) {
//          if (secure) {
//            if (cons != null && (cons.getHeaderField("Content-Length") != null || skipLength)) {
//              if (cons.getHeaderField("Content-Length") != null) {
//                remoteLength = Long.valueOf(cons.getHeaderField("Content-Length"));
//              }
//              if (compareModifiedTime) {
//                remoteModified = sdf.parse(cons.getHeaderField("Last-Modified")).getTime();
//              }
//              inputStream = new BufferedInputStream(cons.getInputStream());
//            }
//          } else {
//            if (con != null && (con.getHeaderField("Content-Length") != null || skipLength)) {
//              if (con.getHeaderField("Content-Length") != null) {
//                remoteLength = Long.valueOf(con.getHeaderField("Content-Length"));
//              }
//              if (compareModifiedTime) {
//                remoteModified = sdf.parse(con.getHeaderField("Last-Modified")).getTime();
//              }
//              inputStream = new BufferedInputStream(con.getInputStream());
//            }
//          }
//          if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
//
//            Common.dbg.println("Downloading file: " + downloadUrl);
//          } else {
//            System.out.println("Downloading file: " + downloadUrl);
//          }
//
//          if (inputStream != null) {
//            writeFile(inputStream, outputStream);
//            inputStream.close();
//          }
//          outputStream.close();
//
//          if (file.length() != remoteLength) {
//            System.out.println("SIZE MISMATCH: Reported - " + remoteLength + ", Downloaded - " + file.length());
//            return false;
//          }
//
//          if (compareModifiedTime && file.setLastModified(remoteModified)) {
//            if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
//              Common.dbg.println("Successfully downloaded!!!");
//            } else {
//              System.out.println("Successfully downloaded!!!");
//            }
//            return true;
//          } else if (!compareModifiedTime) {
//            if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
//              Common.dbg.println("Successfully downloaded!!!");
//            } else {
//              System.out.println("Successfully downloaded!!!");
//            }
//            return true;
//          } else {
//            Common.logMessage("Erorr modifying Last Modified Time "+downloadUrl);
//            Common.warnMessage("Erorr modifying Last Modified Time "+downloadUrl);
//            System.out.println("Erorr modifying Last Modified Time");
//          }
//        } else {
//          Common.logMessage("URL doesn't exist "+downloadUrl);
//          Common.warnMessage("URL doesn't exist "+downloadUrl);
//          System.out.println("URL doesn't exist: " + downloadUrl);
//        }
//      } else {
//        Common.logMessage("URL doesn't exist "+downloadUrl);
//        Common.warnMessage("URL doesn't exist "+downloadUrl);
//        System.out.println("URL doesn't exist: " + downloadUrl);
//      }
//    } catch (IOException | KeyManagementException | NoSuchAlgorithmException | ParseException e) {
//      Common.errorMessage("Error occured during fetch the data from their website");
//      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, e);
//    }
//    skipLength = false;
//    return false;
//  }
public static boolean downloadFileURL(File file, String downloadUrl, boolean skipContentLength) {
  try {

    skipLength = skipContentLength;
    SSLContext sc = SSLContext.getInstance("SSL");
    sc.init(null, trustAllCerts, new java.security.SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(hv);

    downloadUrl = downloadUrl.replaceAll(" ", "%20");
    boolean secure = false;
    if (downloadUrl.contains("https")) {
      secure = true;
    }

    SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
    long remoteLength = 0;
    long remoteModified = 0;
    BufferedInputStream inputStream = null;
    FileOutputStream outputStream = new FileOutputStream(file);

    URL url = new URL(downloadUrl);
    HttpsURLConnection cons = null;
    HttpURLConnection con = null;
    int status;
    boolean compareModifiedTime = false;
    Map<String, List<String>> headerFields;
    if (secure) {
      cons = (HttpsURLConnection) url.openConnection();
      status = cons.getResponseCode();
      headerFields = cons.getHeaderFields();
    } else {
      con = (HttpURLConnection) url.openConnection();
      status = con.getResponseCode();
      headerFields = con.getHeaderFields();
    }

    String fileBName = getFileBasename(downloadUrl);
    if (!lastDownloadedFileName.equals(fileBName) && status == HttpURLConnection.HTTP_NOT_FOUND) {
      String ext = getExtension(downloadUrl);
      lastDownloadedFileName = fileBName;
      url = new URL(downloadUrl.replace(".".concat(ext), ".".concat(ext.toUpperCase())));
      if (secure) {
        cons = (HttpsURLConnection) url.openConnection();
        status = cons.getResponseCode();
        headerFields = cons.getHeaderFields();
      } else {
        con = (HttpURLConnection) url.openConnection();
        status = con.getResponseCode();
        headerFields = con.getHeaderFields();
      }
    }

    if (headerFields.containsKey("Last-Modified")) {
      compareModifiedTime = true;
    }

    if (status == HttpURLConnection.HTTP_OK) {
      if (!headerFields.containsKey("Content-Type") || (headerFields.containsKey("Content-Type") && !headerFields.get("Content-Type").contains("text/html; charset=utf-8"))) {
        if (secure) {
          if (cons != null && (cons.getHeaderField("Content-Length") != null || skipLength)) {
            if (cons.getHeaderField("Content-Length") != null) {
              remoteLength = Long.valueOf(cons.getHeaderField("Content-Length"));
            }
            if (compareModifiedTime) {
              remoteModified = sdf.parse(cons.getHeaderField("Last-Modified")).getTime();
            }
            inputStream = new BufferedInputStream(cons.getInputStream());
          }
        } else {
          if (con != null && (con.getHeaderField("Content-Length") != null || skipLength)) {
            if (con.getHeaderField("Content-Length") != null) {
              remoteLength = Long.valueOf(con.getHeaderField("Content-Length"));
            }
            if (compareModifiedTime) {
              remoteModified = sdf.parse(con.getHeaderField("Last-Modified")).getTime();
            }
            inputStream = new BufferedInputStream(con.getInputStream());
          }
        }
        if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {

          Common.dbg.println("Downloading file: " + downloadUrl);
        } else {
          System.out.println("Downloading file: " + downloadUrl);
        }

        if (inputStream != null) {
          writeFile(inputStream, outputStream);
          inputStream.close();

        }else {
         outputStream.close();
            /** amir's code for dropbox**/
         return downloadDropBox(downloadUrl,file);

        }


        if (file.length() != remoteLength) {
          System.out.println("SIZE MISMATCH: Reported - " + remoteLength + ", Downloaded - " + file.length());
          return false;
        }

        if (compareModifiedTime && file.setLastModified(remoteModified)) {
          if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
            Common.dbg.println("Successfully downloaded!!!");
          } else {
            System.out.println("Successfully downloaded!!!");
          }
          return true;
        } else if (!compareModifiedTime) {
          if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
            Common.dbg.println("Successfully downloaded!!!");
          } else {
            System.out.println("Successfully downloaded!!!");
          }
          return true;
        } else {
          Common.logMessage("Erorr modifying Last Modified Time "+downloadUrl);
          Common.warnMessage("Erorr modifying Last Modified Time "+downloadUrl);
          System.out.println("Erorr modifying Last Modified Time");
        }
      } else {
        Common.logMessage("URL doesn't exist "+downloadUrl);
        Common.warnMessage("URL doesn't exist "+downloadUrl);
        System.out.println("URL doesn't exist: " + downloadUrl);
      }
    } else {
      Common.logMessage("URL doesn't exist "+downloadUrl);
      Common.warnMessage("URL doesn't exist "+downloadUrl);
      System.out.println("URL doesn't exist: " + downloadUrl);
    }
  } catch (IOException | KeyManagementException | NoSuchAlgorithmException | ParseException e) {
    Common.errorMessage("Error occured during fetch the data from their website");
    Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, e);
  }
  skipLength = false;
  return false;
}

  public static boolean isDifferentFromURL(File file, String downloadUrl) {
    if (!file.exists()) {
      return true;
    }
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
      HttpsURLConnection.setDefaultHostnameVerifier(hv);

      downloadUrl = downloadUrl.replaceAll(" ", "%20");
      boolean secure = false;
      if (downloadUrl.contains("https")) {
        secure = true;
      }

      SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");

      URL url = new URL(downloadUrl);
      HttpsURLConnection cons = null;
      HttpURLConnection con = null;
      long remoteLength = 0;
      long remoteModified = 0;
      int status;
      boolean compareModifiedTime = false;
      Map<String, List<String>> headerFields;
      if (secure) {
        cons = (HttpsURLConnection) url.openConnection();
        status = cons.getResponseCode();
        headerFields = cons.getHeaderFields();
      } else {
        con = (HttpURLConnection) url.openConnection();
        status = con.getResponseCode();
        headerFields = con.getHeaderFields();
      }

      String fileBName = getFileBasename(downloadUrl);
      if (!lastDownloadedFileName.equals(fileBName) && status == HttpURLConnection.HTTP_NOT_FOUND) {
        String ext = getExtension(downloadUrl);
        lastDownloadedFileName = fileBName;
        url = new URL(downloadUrl.replace(".".concat(ext), ".".concat(ext.toUpperCase())));
        if (secure) {
          cons = (HttpsURLConnection) url.openConnection();
          status = cons.getResponseCode();
          headerFields = cons.getHeaderFields();
        } else {
          con = (HttpURLConnection) url.openConnection();
          status = con.getResponseCode();
          headerFields = con.getHeaderFields();
        }
      }

      if (headerFields.containsKey("Last-Modified")) {
        compareModifiedTime = true;
      }

      if (status == HttpURLConnection.HTTP_OK) {

        if (secure) {
          if (cons != null && cons.getHeaderField("Content-Length") != null) {
            remoteLength = Long.valueOf(cons.getHeaderField("Content-Length"));
            if (compareModifiedTime) {
              remoteModified = sdf.parse(cons.getHeaderField("Last-Modified")).getTime();
            }
          }
        } else {
          if (con != null && con.getHeaderField("Content-Length") != null) {
            remoteLength = Long.valueOf(con.getHeaderField("Content-Length"));
            if (compareModifiedTime) {
              remoteModified = sdf.parse(con.getHeaderField("Last-Modified")).getTime();
            }
          }
        }
      }

      long localModified = file.lastModified();
      long localLength = file.length();

      if ((compareModifiedTime && localModified != remoteModified) || localLength != remoteLength) {
        System.out.println("MODIFIED: " + downloadUrl + ", downloading now...");
        return true;
      } else {
        System.out.println("NOT MODIFIED: " + downloadUrl + ", skipping download...");
      }
    } catch (MalformedURLException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    } catch (ParseException | NoSuchAlgorithmException | KeyManagementException | IOException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    return false;
  }

  public static boolean isDifferentFromFTP(FTPClient f, File file, String fileName) {
    if (!file.exists()) {
      return true;
    }
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
      String timeArray[] = f.getModificationTime(fileName).split(" ");
      String time = timeArray.length > 1 ? timeArray[1] : timeArray[0];

      long remoteLength = 0;
      long remoteModified = sdf.parse(time).getTime();
      FTPFile[] files = f.listFiles(fileName);
      if (files.length == 1 && files[0].isFile()) {
        remoteLength = files[0].getSize();
      }

      long localModified = file.lastModified();
      long localLength = file.length();

      if (localModified != remoteModified || localLength != remoteLength) {
        System.out.println("MODIFIED: " + fileName + ", downloading now...");
        return true;
      } else {
        System.out.println("NOT MODIFIED: " + fileName + ", skipping download...");
      }
    } catch (IOException | ParseException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    return false;
  }

  public static List<String> getFTPFileList(FTPClient f, String parentDir, String currentDir, Integer limit, List<String> files) {
    String dirToList = parentDir;
    if (!currentDir.equals("")) {
      dirToList += "/" + currentDir;
    }
    if(loopTrackingFlag) {
      Common.startMessage("Fetch");
      Common.sizeMessage(1);

      loopTrackingFlag=false;
    }
    try {
      FTPFile[] subFiles = f.listFiles(dirToList);
      Common.targetMessage(subFiles.length);
      if (subFiles != null && subFiles.length > 0) {
        for (FTPFile aFile : subFiles) {
          String currentFileName = aFile.getName();
          if (currentFileName.equals(".") || currentFileName.equals("..")) {
            continue;
          }
          String filePath = parentDir + "/" + ("".equals(currentDir) ? "" : currentDir + "/") + currentFileName;
          if (aFile.isDirectory()) {
            getFTPFileList(f, dirToList, aFile.getName(), limit, files);
          } else if (aFile.isFile()) {
            files.add(filePath);
            if (files.size() >= limit) {
              break;
            }
          }
          countMessageFileCounter++;
          Common.countMessage(countMessageFileCounter);
        }
      }
    } catch (IOException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    Common.endMessage("Fetch");
    return files;
  }

  public static List<String> getFTPFileList(FTPClient f, String parentDir, String currentDir, Integer limit) {
    List<String> files = new ArrayList<>();
    files = getFTPFileList(f, parentDir, currentDir, limit, files);
    return files;
  }

  public static List<String> getFTPFileList(FTPClient f, String parentDir, String currentDir) {
    return getFTPFileList(f, parentDir, currentDir, Integer.MAX_VALUE);
  }

  public static List<String> downloadFolderFTP(FTPClient f, String parentDir, String currentDir, String saveDir, int fileLimit) throws IOException {
    List<String> downloadedFiles = new ArrayList<>();
    String dirToList = parentDir;
    if (!currentDir.equals("")) {
      dirToList += "/" + currentDir;
    }
    f.setKeepAlive(true);
    System.out.println("Connected waiting for list...");
    FTPFile[] subFiles = f.listFiles(dirToList);
    if(loopTrackingFlag) {
      Common.startMessage("FETCH");
      Common.sizeMessage(1);
      countMessageFileCounter=0;
      targetMessageFileCounter=0;
      countFolderFolderFTP(f,parentDir,currentDir,saveDir,fileLimit);
      Common.targetMessage(targetMessageFileCounter);
      loopTrackingFlag=false;
    }
    System.out.println("Number of files in " + dirToList + ": " + (subFiles != null ? subFiles.length : 0));
    if (subFiles != null && subFiles.length > 0) {
      int fileCount = 0;
      boolean limit = false;
      if (fileLimit > 0) {
        limit = true;
      }
      for (FTPFile aFile : subFiles) {
        String currentFileName = aFile.getName();
        if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
          Common.dbg.println("File: " + currentFileName);
        } else {
          System.out.println("File: " + currentFileName);
        }
        if (currentFileName.equals(".") || currentFileName.equals("..")) {
          continue;
        }
        String filePath = parentDir + "/" + currentDir + "/" + currentFileName;
        if (currentDir.equals("")) {
          if (fileLimit == -1) {
            filePath = parentDir;
          } else {
            filePath = parentDir + "/" + currentFileName;
          }
        }

        String newDirPath = saveDir + parentDir;
        if (fileLimit != -1) {
          newDirPath += File.separator + (currentDir.equals("") ? "" : currentDir + File.separator) + currentFileName;
        }

        if (aFile.isDirectory()) {
          if (!limit) {
            new File(newDirPath + File.separator).mkdirs();
            downloadedFiles.addAll(downloadFolderFTP(f, dirToList, currentFileName, saveDir, fileLimit));
          }
        } else {

          if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
            Common.dbg.println("Checking File: " + filePath);
          } else {
            System.out.println("Checking File: " + filePath);
          }
          if (isDifferentFromFTP(f, new File(newDirPath), filePath)) {
            boolean success = downloadFileFTP(f, filePath, newDirPath);
            if (success) {
              downloadedFiles.add(newDirPath);
              if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
                Common.dbg.println("Successfully downloaded!!!");
              } else {
                System.out.println("Successfully downloaded!!!");
              }
            }
            fileCount++;
            if (limit && fileCount == fileLimit) {
              break;
            }
          }
        }

      }

    }
    countMessageFileCounter++;
    Common.countMessage(countMessageFileCounter);
    if(countMessageFileCounter==targetMessageFileCounter) {
      Common.endMessage("FETCH");
      countMessageFileCounter=0;
      targetMessageFileCounter=0;
      loopTrackingFlag=true;

    }
    return downloadedFiles;
  }

  public static List<String> getFilesList(String folder) {
    List<String> list = new ArrayList<>();
    String[] fileList = new File(folder).list();
    for (String file : fileList) {
      File fileObj = new File(folder.concat(File.separator).concat(file));
      if (fileObj.isDirectory()) {
        list.addAll(getFilesList(fileObj.getPath()));
      } else {
        list.add(fileObj.getPath());
      }
    }
    return list;
  }

  public static String getFileBasename(String fileName) {
    if (fileName.contains("/")) {
      fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
    }
    if (fileName.contains("\\")) {
      fileName = fileName.substring(fileName.lastIndexOf("\\") + 1);
    }
    return fileName;
  }

  @SuppressWarnings("unchecked")
  public static List<String> downloadFolderSFTP(ChannelSftp c, String parentDir, String currentDir, String saveDir, boolean recurse, List<String> fileNamePatterns, boolean appendParentDirs) {
    List<String> downloadedFiles = new ArrayList<>();
    int  countDownload=0;
    Common.startMessage("Fetch");
    Common.sizeMessage(1);

    try {
      String dirToList = parentDir;
      if (!currentDir.equals("")) {
        dirToList += "/" + currentDir;
      }
      Vector<LsEntry> ls = c.ls(dirToList.equals("") ? "/" : dirToList);
      Common.targetMessage(ls.size());
      System.out.println("Number of files in '" + dirToList + "': " + ls.size());
      if (ls.size() > 0) {
        for (LsEntry l : ls) {
          SftpATTRS attrs = l.getAttrs();
          String currentFileName = l.getFilename();
          if (currentFileName.equals(".") || currentFileName.equals("..")) {
            continue;
          }
          String filePath = parentDir + File.separator + (currentDir.equals("") ? "" : currentDir + File.separator) + currentFileName;
          String newDirPath = appendParentDirs ? saveDir + parentDir + File.separator + (currentDir.equals("") ? "" : currentDir + File.separator) + currentFileName : saveDir + File.separator + currentFileName;
          if (attrs.isDir() && recurse) {
            new File(newDirPath + File.separator).mkdirs();
            downloadedFiles.addAll(downloadFolderSFTP(c, dirToList, currentFileName, saveDir, recurse, fileNamePatterns, appendParentDirs));
          } else if (!attrs.isLink()) {
            if (fileNamePatterns.isEmpty() || (!fileNamePatterns.isEmpty() && matchesPattern(currentFileName, fileNamePatterns))) {
              if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
                Common.dbg.println("Checking File: " + filePath);
              } else {
                System.out.println("Checking File: " + filePath);
              }

              long remoteModified = (long) attrs.getMTime() * 1000;
              long remoteLength = attrs.getSize();
              File newFile = new File(newDirPath);
              boolean different = true;

              if (newFile.exists()) {
                long localModified = newFile.lastModified();
                long localLength = newFile.length();

                if (localModified != remoteModified || localLength != remoteLength) {
                  System.out.println("MODIFIED: " + filePath + ", downloading now...");
                } else {
                  different = false;
                  System.out.println("NOT MODIFIED: " + filePath + ", skipping download...");
                }
              } else {
                System.out.println("MODIFIED: " + filePath + ", downloading now...");
              }

              if (different) {
                boolean success = false;

                try {
                  BufferedInputStream inputStream = new BufferedInputStream(c.get(filePath));
                  newFile.getParentFile().mkdirs();
                  OutputStream os = new FileOutputStream(newFile);
                  BufferedOutputStream outputStream = new BufferedOutputStream(os);

                  writeFile(inputStream, outputStream);

                  outputStream.close();
                  inputStream.close();

                  if (newFile.length() != remoteLength) {
                    success = false;
                    System.out.println("SIZE MISMATCH: Reported - " + remoteLength + ", Downloaded - " + newFile.length());
                  } else if (newFile.setLastModified(remoteModified)) {
                    success = true;
                  } else {
                    success = false;
                    System.out.println("Erorr modifying Last Modified Time");
                  }
                  /* end if (newFile.length() != remoteLength) else */

                } catch (SftpException | IOException ex) {
                  Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
                } /* end try */

                if (success) {
                  downloadedFiles.add(newDirPath);
                  if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
                    Common.dbg.println("Successfully downloaded!!!");
                  } else {
                    System.out.println("Successfully downloaded!!!");
                  }
                } /* end if (success) */

              }
            }
          }
          countDownload++;
          Common.countMessage(countDownload);
        }
      }
    } catch (SftpException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    Common.endMessage("Fetch");
    return downloadedFiles;
  }

  public static String joinList(List<String> list, String joinWith) {
    StringBuilder result = new StringBuilder();
    for (String item : list) {
      if (result.length() > 0) {
        result.append(joinWith);
      }
      result.append(item);
    }
    return result.toString();
  }

  public static boolean matchesPattern(String string, List<String> list) {
    for (String s : list) {
      if (string.contains(s)) {
        return true;
      }
    }
    return false;
  }

  private static String downloadedSize(long progress) {
    String units = " B";
    if (progress / 1024 >= 10) {
      units = " KB";
      progress = progress / 1024;
      if (progress / 1024 >= 10) {
        units = " MB";
        progress = progress / 1024;
        if (progress / 1024 >= 10) {
          units = " GB";
          progress = progress / 1024;
        }
      }
    }
    return String.format("%10s", progress + units);
  }

  private static void writeFile(InputStream in, OutputStream out) throws IOException {
    byte[] bytesArray = new byte[1024];
    int bytesRead;
    long progress = 0;
    while ((bytesRead = in.read(bytesArray, 0, 1024)) != -1) {
      out.write(bytesArray, 0, bytesRead);
      progress += bytesRead != -1 ? bytesRead : 0;
      if (System.getenv("HOME") != null && System.getenv("HOME").equalsIgnoreCase("/home/realacqcom")) {
        Common.dbg.print(" ...writing " + downloadedSize(progress) + "\r");
      } else {
        System.out.print(" ...writing " + downloadedSize(progress) + "\r");
      }
    }
  }

  public static List<String> downloadListFilesystem(Map<String, String> fileList) {
    List<String> modified = new ArrayList<>();
    for (String location : fileList.keySet()) {
      String filePath = fileList.get(location);
      boolean isDifferent = true;
      File file = new File(filePath);
      File bakFile = new File(filePath.concat(".bak"));

      if (file.exists()) {
        isDifferent = isDifferentFromFilesystem(file, location);
      }

      if (isDifferent) {
        if (bakFile.exists()) {
          bakFile.delete();
        }
        if (file.exists()) {
          try {
            Files.move(file.toPath(), bakFile.toPath(), REPLACE_EXISTING);
          } catch (IOException ex) {
            Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException ex) {
          Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
          Files.move(new File(location).toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
          modified.add(filePath);
        } catch (IOException ex) {
          Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }
    return modified;
  }

  public static boolean isDifferentFromFilesystem(File file, String downloadUrl) {
    if (!file.exists()) {
      return true;
    }
    try {
      if (!FileUtils.contentEquals(file, new File(downloadUrl))) {
        System.out.println("MODIFIED: " + downloadUrl + ", downloading now...");
        return true;
      } else {
        System.out.println("NOT MODIFIED: " + downloadUrl + ", skipping download...");
      }
    } catch (IOException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
    }
    return false;
  }

  public static boolean recreateZipFile(String zipFile) {
    System.out.println("Recreating Zip file");
    File file = new File(zipFile);

    System.out.println("Unzipping ".concat(zipFile).concat(" to ").concat(file.getParent()).concat(File.separator).concat("tmp"));
    ProcessBuilder pb = new ProcessBuilder("unzip", zipFile, "-d", file.getParent().concat(File.separator).concat("tmp"));
    try {
      Process pr = pb.start();
      pr.waitFor();
      Files.move(new File(zipFile).toPath(), new File(zipFile.concat(".").concat("BAK")).toPath(), StandardCopyOption.REPLACE_EXISTING);
      System.out.println("Zipping ".concat(file.getParent()).concat(File.separator).concat("tmp").concat(" to ").concat(zipFile));
      List<String> commands = new ArrayList<>();
      commands.add("zip");
      commands.add("-r");
      commands.add("-j");
      commands.add(zipFile);
      commands.add(file.getParent().concat(File.separator).concat("tmp"));
      pb = new ProcessBuilder(commands);
      pr = pb.start();
      pr.waitFor();
      System.out.println("Removing tmp directory");
      rmdir(new File(file.getParent().concat(File.separator).concat("tmp")));
      System.out.println("Removing backup zip file");
      new File(zipFile.concat(".").concat("BAK")).delete();
    } catch (IOException | InterruptedException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
      try {
        System.out.println("Restoring backup zip file");
        Files.move(new File(zipFile.concat(".").concat("BAK")).toPath(), new File(zipFile).toPath(), StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException ex1) {
        Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex1);
      }
      return false;
    }
    return true;
  }

  /*
   * Empty and delete a folder (and sub folders).
   *
   * @param folder folder to empty
   */
  public static void rmdir(final File folder) {
    // check if folder file is a real folder
    if (folder.isDirectory()) {
      File[] list = folder.listFiles();
      if (list != null) {
        for (File tmpF : list) {
          if (tmpF.isDirectory()) {
            rmdir(tmpF);
          }
          tmpF.delete();
        }
      }
      if (!folder.delete()) {
        System.out.println("can't delete folder : " + folder);
      }
    }
  }

  public static String getExtension(File file) {
    return getExtension(file.getName());
  }

  public static String getExtension(String file) {
    String name = getFileBasename(file);
    return name.contains(".") ? name.substring(name.lastIndexOf(".") + 1) : "";
  }

  public static boolean buildTarFile(String backupFilePath, File directory, String contents) {
    try {
      System.out.println("Building backup tgz file " + backupFilePath + " from " + contents);
      List<String> commands = new ArrayList<>();

      /*
       * Create an include-file containing list of all images
       * This is to overcome "Argument list too long error"
       */
      commands.add("bash");
      commands.add("-c");
      commands.add("find . -type f -name \"" + contents + "\" > include-file");
      ProcessBuilder pb = new ProcessBuilder(commands);
      pb.directory(directory);
      pb.redirectErrorStream(true);
      Process pr = pb.start();
      InputStream output = pr.getInputStream();
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(output), 1);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println(line);
      }
      pr.waitFor();

      /*
       * Create the tar gzipped archive from the include-file
       */
      commands.clear();
      commands.add("bash");
      commands.add("-c");
      commands.add("tar -czf " + backupFilePath + " -T include-file");
      pb = new ProcessBuilder(commands);
      pb.directory(directory);
      pb.redirectErrorStream(true);
      pr = pb.start();
      output = pr.getInputStream();
      bufferedReader = new BufferedReader(new InputStreamReader(output), 1);
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println(line);
      }
      pr.waitFor();

      /*
       * Delete the include-file
       */
      commands.clear();
      commands.add("bash");
      commands.add("-c");
      commands.add("rm include-file");
      pb = new ProcessBuilder(commands);
      pb.directory(directory);
      pb.redirectErrorStream(true);
      pr = pb.start();
      output = pr.getInputStream();
      bufferedReader = new BufferedReader(new InputStreamReader(output), 1);
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println(line);
      }
      pr.waitFor();

      System.out.println("Process complete.");

      return true;
    } catch (IOException | InterruptedException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
      return false;
    }
  }

  public static boolean buildZipFile(String zipFilePath, File directory, String contents, boolean moveOriginal) {
    try {
      System.out.println("Building zip file " + zipFilePath + " from " + contents);
      List<String> commands = new ArrayList<>();
      commands.add("bash");
      commands.add("-c");
      commands.add("find . -type f -name \"" + contents + "\" -print | zip " + (moveOriginal ? "-m " : "") + "-T -j " + zipFilePath + " -@");
      ProcessBuilder pb = new ProcessBuilder(commands);
      pb.directory(directory);
      pb.redirectErrorStream(true);
      Process pr = pb.start();
      InputStream output = pr.getInputStream();
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(output), 1);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println(line);
      }
      pr.waitFor();

      System.out.println("Process complete.");

      return true;
    } catch (IOException | InterruptedException ex) {
      Logger.getLogger(FetchHelper.class.getName()).log(Level.SEVERE, null, ex);
      return false;
    }
  }
  public static List<String> countFolderFolderFTP(FTPClient f, String parentDir, String currentDir, String saveDir, int fileLimit) throws IOException {
    List<String> downloadedFiles = new ArrayList<>();
    String dirToList = parentDir;
    if (!currentDir.equals("")) {
      dirToList += "/" + currentDir;
    }
    f.setKeepAlive(true);
    FTPFile[] subFiles = f.listFiles(dirToList);
    if (subFiles != null && subFiles.length > 0) {
      boolean limit = false;
      if (fileLimit > 0) {
        limit = true;
      }
      for (FTPFile aFile : subFiles) {
        String currentFileName = aFile.getName();
        if (currentFileName.equals(".") || currentFileName.equals("..")) {
          continue;
        }
        String filePath = parentDir + "/" + currentDir + "/" + currentFileName;
        if (currentDir.equals("")) {
          if (fileLimit == -1) {
            filePath = parentDir;
          } else {
            filePath = parentDir + "/" + currentFileName;
          }
        }

        String newDirPath = saveDir + parentDir;
        if (fileLimit != -1) {
          newDirPath += File.separator + (currentDir.equals("") ? "" : currentDir + File.separator) + currentFileName;
        }
        if (aFile.isDirectory()) {
          if (!limit) {
            new File(newDirPath + File.separator).mkdirs();
            downloadedFiles.addAll(countFolderFolderFTP(f, dirToList, currentFileName, saveDir, fileLimit));
          }
        }
      }
    }
    targetMessageFileCounter++;
    return downloadedFiles;
  }

}
