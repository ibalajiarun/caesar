package hyflow.benchmark.bank;

import hyflow.transaction.AbstractObject;

/**
 * Created by balajiarun on 3/7/16.
 */
public class Account extends AbstractObject {

    private final int id;

    private int balance;

    public Account(int id, int initBalance) {
        super();
        this.balance = initBalance;
        this.id = id;
    }

    public int getId() {
        return this.id;
    }

    public int getAmount() {
        return balance;
    }

    public void set(int newBalance) {
        balance = newBalance;
    }

    public void withdraw(int amount) {
        balance -= amount;
    }

    public void deposit(int amount) {
        balance += amount;
    }
}

