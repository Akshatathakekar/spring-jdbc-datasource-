package com.capgemini.bankappDao;

import java.util.List;

import com.capgemini.bankapp.exception.BankAccountNotFoundException;
import com.capgemini.model.BankAccount;

public interface BankAccountDao {

	
	public double getBalance(long accountId) ;
	public void updateBalance(long accountId,double newBalance);
	public boolean deleteBankaccount(long accountId);
	public boolean addNewbankAccount(BankAccount account);
	public List<BankAccount> findAllbankAccounts();
	public BankAccount searchAccount(long accountId);
	public boolean updateAccount(long accountId,String accountHolderName,String accountType);
	public void commit();
	public void rollback();
}
