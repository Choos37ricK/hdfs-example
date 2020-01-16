public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args)
    {
        String rootPath = "hdfs://e355d5b8baf0:8020/";

        try {
            FileAccess fileAccess = new FileAccess(rootPath);

            //fileAccess.create(rootPath + "createMethod.txt");
            //fileAccess.append(rootPath + "createMethod.txt", "hello, reverse this: Nortoza Forhnis");
            //fileAccess.read(rootPath + "createMethod.txt");
            //fileAccess.delete(rootPath + "folder/");
            //System.out.println("Path is directory: " + fileAccess.isDirectory(rootPath + "createMethod.txt/"));
            fileAccess.list(rootPath).forEach(System.out::println);

            fileAccess.closeHDFS();
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        FileSystem hdfs = FileSystem.get(
            new URI("hdfs://HOST_NAME:8020"), configuration
        );
        Path file = new Path("hdfs://HOST_NAME:8020/test/file.txt");

        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }

        OutputStream os = hdfs.create(file);
        BufferedWriter br = new BufferedWriter(
            new OutputStreamWriter(os, "UTF-8")
        );

        for(int i = 0; i < 10_000_000; i++) {
            br.write(getRandomWord() + " ");
        }

        br.flush();
        br.close();
        hdfs.close();*/
    }

    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
