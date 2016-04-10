package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.benchmark.bank.Bank;
import hyflow.caesar.Caesar;
import hyflow.caesar.replica.Replica;
import hyflow.common.Configuration;
import hyflow.common.ProcessDescriptor;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ForkJoinPool;

/**
 * Created by balajiarun on 3/6/16.
 */
public class Main {

    private static Logger logger = LogManager.getLogger(Main.class);

    public static Options buildOptions() {
        Option helpOpt = new Option("h", "help", false, "print this message");

        Option clientCountOpt = Option.builder("c")
                .longOpt("clients")
                .hasArg()
                .desc("number of clients")
                .required()
                .build();

        Option replicaIdOpt = Option.builder("id")
                .longOpt("id")
                .hasArg()
                .desc("replica id")
                .required()
                .build();

        Option testTimeOpt = Option.builder("t")
                .longOpt("test-time")
                .hasArg()
                .desc("test time")
                .required()
                .build();

        Option benchmarkOpt = Option.builder("b")
                .longOpt("benchmark")
                .hasArg()
                .desc("benchmark service class")
                .required()
                .build();

        Option benchConfigOpt = Option.builder("bc")
                .longOpt("benchconfig")
                .hasArg()
                .desc("benchmark config")
                .required()
                .build();

        Option propFileOpt = Option.builder("p")
                .longOpt("propfile")
                .hasArg()
                .desc("caesar properties file")
                .required()
                .build();

        Options options = new Options();

        options.addOption(helpOpt);
        options.addOption(clientCountOpt);
        options.addOption(replicaIdOpt);
        options.addOption(testTimeOpt);
        options.addOption(benchmarkOpt);
        options.addOption(benchConfigOpt);
        options.addOption(propFileOpt);

        return options;
    }

    public static void main(String[] args) throws Exception {

        final String USAGE = "caesar [-h] -id <id> -c <clients> -t <test time> -b <benchmark> -bc <benchmark config> -p <properties file>";

        Options options = buildOptions();

        CommandLine line;
        try {
            line = new DefaultParser().parse(options, args);

            int localId = Integer.parseInt(line.getOptionValue("id"));
            int numClients = Integer.parseInt(line.getOptionValue("c"));
            int roundTime = Integer.parseInt(line.getOptionValue("t"));

            String benchFile = line.getOptionValue("bc");
            String propFile = line.getOptionValue("p");

            logger.fatal("Pool Size " + ForkJoinPool.getCommonPoolParallelism());

            Configuration config = new Configuration(propFile);

            ProcessDescriptor.initialize(config, localId);
            AbstractService service = new Bank(benchFile);
            Caesar caesar = new Caesar(service.getTotalObjects());
            Replica replica = new Replica(service, caesar);

            ClientManager client = new ClientManager(numClients, localId, service, replica, caesar);

            replica.start(client);

            Thread.sleep(10000);

            System.out.println("Start Barrier");
            caesar.enterBarrier("start", ProcessDescriptor.getInstance().numReplicas);
            System.out.println("Started");

            client.collectStats(roundTime);

            System.in.read();

            System.exit(0);


        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            new HelpFormatter().printHelp(USAGE, options);
        }


    }

}
