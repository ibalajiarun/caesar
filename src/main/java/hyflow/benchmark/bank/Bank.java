package hyflow.benchmark.bank;

import hyflow.benchmark.AbstractBenchmark;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.transaction.SharedObjectRegistry;
import hyflow.transaction.TransactionType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by balajiarun on 3/21/16.
 */
public class Bank extends AbstractBenchmark {

    private static final int INITIAL_BALANCE = 1000;
    private static final int DEFAULT_TRANSACTION_AMOUNT = 10;
    private static final Logger logger = LogManager.getLogger(Bank.class);
    private final int numAccounts;
    private final SharedObjectRegistry registry;

    public Bank(String fileName) throws IOException {
        super(fileName);

        numAccounts = Integer.parseInt(configuration.getProperty("numAccounts", "500"));

        this.registry = new SharedObjectRegistry(numAccounts);

        for (int id = 0; id < this.numAccounts; id++) {
            Account account = new Account(INITIAL_BALANCE, id);
            this.registry.registerObjects(id, account);
        }
    }

    public int getBalance(int src, int dst) {

        Account srcAccount, dstAccount;

        srcAccount = (Account) registry.getObject(src);
        dstAccount = (Account) registry.getObject(dst);

        int balance = srcAccount.getAmount() + dstAccount.getAmount();
        return balance;
    }

    public boolean transfer(int src, int dst) {

        Account srcAccount, dstAccount;

        srcAccount = (Account) registry.getObject(src);
        dstAccount = (Account) registry.getObject(dst);
        srcAccount.withdraw(DEFAULT_TRANSACTION_AMOUNT);
        dstAccount.deposit(DEFAULT_TRANSACTION_AMOUNT);
        return true;
    }

    @Override
    public Request createRequest(RequestId rId, boolean read) {
        final int PAYLOAD_SIZE = 10;
        final Random random = new Random();

        Request request;
        byte[] payload = new byte[PAYLOAD_SIZE];
        int[] objectIds = new int[2];
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int src, dst;

        if (read) {
            buffer.put((byte) TransactionType.ReadOnlyTransaction.ordinal());
            buffer.put((byte) OpType.GetBalance.ordinal());
        } else {
            buffer.put((byte) TransactionType.ReadWriteTransaction.ordinal());
            buffer.put((byte) OpType.Transfer.ordinal());
        }

        src = random.nextInt(this.numAccounts);
        objectIds[0] = src;
        do {
            dst = random.nextInt(this.numAccounts); //random.nextInt(max - min) + min;
        } while (src >= dst);
        objectIds[1] = dst;

        buffer.putInt(src);
        buffer.putInt(dst);

        buffer.flip();

        request = new Request(rId, objectIds, payload);
        return request;
    }

    @Override
    public void executeRequest(final Request request) {
        byte[] value = request.getPayload();
        ByteBuffer buffer = ByteBuffer.wrap(value);

        buffer.get(); // TransactionType
        OpType command = OpType.values()[buffer.get()];

        final int src = buffer.getInt();
        final int dst = buffer.getInt();

        if (command == OpType.Transfer)
            transfer(src, dst);
        else
            getBalance(src, dst);
    }

    public enum OpType {
        Transfer,
        GetBalance
    }

}
