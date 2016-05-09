package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.caesar.Caesar;
import hyflow.caesar.replica.Replica;
import hyflow.common.Configuration;
import hyflow.common.ProcessDescriptor;
import org.apache.commons.cli.*;

import java.lang.reflect.Constructor;

/**
 * Created by balajiarun on 3/6/16.
 */
public class Main {

    public static Options buildOptions() {
        Option helpOpt = new Option("h", "help", false, "print this message");

        Option clientCountOpt = Option.builder("c")
                .longOpt("client")
                .hasArg()
                .desc("client name")
                .required()
                .build();

        Option replicaIdOpt = Option.builder("id")
                .longOpt("id")
                .hasArg()
                .desc("replica id")
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
        options.addOption(benchmarkOpt);
        options.addOption(benchConfigOpt);
        options.addOption(propFileOpt);

        return options;
    }

    public static void main(String[] args) throws Exception {

        final String USAGE = "caesar [-h] -id <id> -c <clients> -b <benchmark> -bc <benchmark config> -p <properties file>";

        Options options = buildOptions();

        CommandLine line;
        try {
            line = new DefaultParser().parse(options, args, false);

            int localId = Integer.parseInt(line.getOptionValue("id"));

            String clientName = line.getOptionValue("c");
            String bench = line.getOptionValue("b");
            String benchFile = line.getOptionValue("bc");
            String propFile = line.getOptionValue("p");

            System.setProperty("id", String.valueOf(localId));

            Configuration config = new Configuration(propFile);

            ProcessDescriptor.initialize(config, localId);

            Class<?> benchClass = Class.forName(bench);
            Constructor<?> constructor = benchClass.getConstructor(String.class);
            AbstractService service = (AbstractService) constructor.newInstance(benchFile);

            Caesar caesar = new Caesar(service.getTotalObjects());
            Replica replica = new Replica(service, caesar);

            Class<?> cClass = Class.forName(clientName);
            Constructor<?> cConstructor = cClass.getConstructor(int.class, AbstractService.class, Caesar.class);
            Client client = (Client) cConstructor.newInstance(localId, service, caesar);

            replica.start(client);

            Thread.sleep(5000);

            System.out.println("Start Barrier");
            caesar.enterBarrier("start", ProcessDescriptor.getInstance().numReplicas);
            System.out.println("Started");

            client.run();

            System.exit(0);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            new HelpFormatter().printHelp(USAGE, options);
        }


    }

}
