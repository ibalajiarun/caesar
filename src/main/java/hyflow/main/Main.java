package hyflow.main;

import hyflow.caesar.Caesar;
import hyflow.caesar.replica.Replica;
import hyflow.common.Configuration;
import hyflow.common.ProcessDescriptor;
import hyflow.service.Service;
import org.apache.commons.cli.*;

/**
 * Created by balajiarun on 3/6/16.
 */
public class Main {

    public static Options buildOptions() {
        Option helpOpt = new Option("h", "help", false, "print this message");

        Option clientCountOpt = Option.builder("c")
                .longOpt("clients")
                .hasArg()
                .desc("number of clients")
                .required()
                .build();

        Option replicaIdOpt = Option.builder("i")
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

//        Option benchmarkOpt = Option.builder("b")
//                .longOpt("benchmark")
//                .hasArg()
//                .desc("benchmark service class")
//                .required()
//                .build();
//
//        Option bankObjectsOpt = Option.builder("a")
//                .longOpt("accounts")
//                .hasArg()
//                .desc("number of bank accounts")
//                .build();

        Options options = new Options();

        options.addOption(helpOpt);
        options.addOption(clientCountOpt);
        options.addOption(replicaIdOpt);
        options.addOption(testTimeOpt);
//        options.addOption(benchmarkOpt);

//        options.addOption(bankObjectsOpt);

        return options;
    }

    public static void main(String[] args) throws Exception {

        final String USAGE = "caesar [-h] -i <id> -c <clients> -t <test time> -b <benchmark> [-a accounts]";

        Options options = buildOptions();

//        String[] args1 = new String[] {"-i", "0", "-t", "1", "-c", "1"};
        CommandLine line;
        try {
            line = new DefaultParser().parse(options, args);

            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Configuration config = new Configuration(loader.getResource("caesar.properties"));

            int localId = Integer.parseInt(line.getOptionValue("i"));

            ProcessDescriptor.initialize(config, localId);
            Service service = new RequestProducer();
            Caesar caesar = new Caesar();

            ClientManager client = new ClientManager(Integer.parseInt(line.getOptionValue("c")), localId, service, caesar);
            Replica replica = new Replica(config, localId, service, caesar);

            replica.start();

            Thread.sleep(10000);

            client.start();

            System.in.read();

            System.exit(0);


//            switch (line.getOptionValue("b")) {
//                case "bank":
//                    if(!line.hasOption("a"))
//                        throw new MissingOptionException("Missing option for benchmark bank: a");
//
//                    break;
//                default:
//                    throw new IllegalArgumentException("Invalid argument for option b");
//            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            new HelpFormatter().printHelp(USAGE, options);
        }


    }



}
