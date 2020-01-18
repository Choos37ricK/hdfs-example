public class Main
{
    //private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) {
        String rootPath = "hdfs://9aa286ed760b:8020/";
        String localPath = "subconscious.txt";
        String hdfsFilePath = rootPath + "subconscious.txt";

        try {
            FileAccess fileAccess = new FileAccess(rootPath);

            fileAccess.copyFromLocalFile(localPath, hdfsFilePath);
            //fileAccess.copyToLocalFile(hdfsFilePath, localPath);

            //fileAccess.create(rootPath + "createMethod.txt");
            //fileAccess.append(rootPath + "createMethod.txt", "hello, reverse this: Nortoza Forhnis");
            //System.out.println("Readed content: " + fileAccess.read(rootPath + "createMethod.txt"));
            //fileAccess.delete(rootPath + "folder/");
            //System.out.println("Path is directory: " + fileAccess.isDirectory(rootPath + "createMethod.txt/"));
            //fileAccess.list(rootPath).forEach(System.out::println);

            fileAccess.closeHDFS();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
