import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileAccess
{
    private static Configuration configuration;
    private static FileSystem hdfs;
    private static String rootPath;

    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        this.rootPath = rootPath;
        configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");
        hdfs = FileSystem.get(new URI(rootPath), configuration);
    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws IOException {
        Path destionationPath = new Path(path);

        if (hdfs.exists(destionationPath)) {
            hdfs.delete(destionationPath, true);
        }
        hdfs.create(destionationPath);
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException {
        Path destionationPath = new Path(path);

        if (!hdfs.exists(destionationPath)) {
            create(path);
        }

        OutputStream os = hdfs.append(destionationPath);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        bw.write(content);
        bw.flush();
        bw.close();
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {
        Path destionationPath = new Path(path);

        FSDataInputStream inputStream = hdfs.open(destionationPath);
        String content = IOUtils.toString(inputStream);
        System.out.println("Readed content: " + content);
        inputStream.close();
        return null;
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws IOException {
        Path destionationPath = new Path(path);

        if (!hdfs.exists(destionationPath)) {
            throw new IOException("File/directory doesn't exists!");
        }
        hdfs.delete(destionationPath, true);
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws Exception {
        Path destionationPath = new Path(path);

        if (hdfs.isDirectory(destionationPath))
            return true;
        else if (hdfs.isFile(destionationPath))
            return false;
        else throw new Exception("What is it else?!");
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws IOException {
        Path destionationPath = new Path(path);

        List<String> childsList = new ArrayList<>();
        FileStatus[] childElements = hdfs.listStatus(destionationPath);
        Arrays.stream(childElements).forEach(child -> childsList.add(child.getPath().getName()));
        return childsList;
    }

    public void closeHDFS() throws IOException {
        hdfs.close();
    }
}
