package control;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Search {

    private final int port;
    private static Logger logger= LoggerFactory.getLogger(Search.class);

    private static Options opts = new Options();
    static {
        // 配置两个参数
        // -h --help 帮助文档
        // -c --config config参数
        opts.addOption("h", "help", false, "The command help");
        opts.addOption("c", "config", true, "Use config file.");
        opts.addOption("p","port",true,"Bind port.");
    }

    private static void printHelp(Options opts) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("The hcimg search server.", opts);
    }

    private Search(int port) {this.port = port;}

    private void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new SearchInitializer());

            Channel ch = b.bind(port).sync().channel();
            logger.info(""+SearchGlobal.getInstance().getProcessors()+" processors available.");
            System.out.print("HC Image Search Server (Designed by Li Xiang).\n");
            System.out.print("Using LIRE for CBIR.\n");
            System.out.print(" _   _ _____ _____ _____\n"
                    +"| | | |_   _/  ___/  ___|\n"
                    +"| |_| | | | \\ `--.\\ `--.\n"
                    +"|  _  | | |  `--. \\`--. \\\n"
                    +"| | | |_| |_/\\__/ /\\__/ /\n"
                    +"\\_| |_/\\___/\\____/\\____/\n");
            logger.info("Listening on port " + port);

                    ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {

        SearchGlobal sg=SearchGlobal.getInstance();

        // 解析参数
        String configFile=Define.DEFAULT_CONFIG;
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cl = parser.parse(opts, args);
            if(cl.hasOption("h")) {
                printHelp(opts);
                return;
            }
            if(cl.hasOption("c")) {
                configFile = cl.getOptionValue("c");
                logger.info("Use config file: "+configFile);
            }
            if(cl.hasOption("p")){
                String portStr = cl.getOptionValue("p");
                sg.setPort(Integer.valueOf(portStr));
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        if(!sg.load(configFile)){
            logger.error("Server initialize failed.");
            System.exit(1);
            return;
        }
        int port=sg.getPort();

        new Search(port).run();

        //exit with no error
        System.exit(0);
    }
}