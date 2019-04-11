package com.capgemini.bankapp.service.impl;

import java.util.List;

import javax.security.auth.login.AccountNotFoundException;

//import org.apache.log4j.Logger;

import com.capgemini.bankapp.dao.impl.BankAccountDaoImpl;
import com.capgemini.bankapp.exception.BankAccountNotFoundException;
import com.capgemini.bankapp.exception.LowBalanceException;
import com.capgemini.bankapp.service.BankAccountService;

import com.capgemini.bankappDao.BankAccountDao;
import com.capgemini.model.BankAccount;

public class BankAccountServiceImpl implements BankAccountService{

	BankAccountDao bankdeo;
	//static final Logger logger=Logger.getLogger(Dbutil.class);
	public BankAccountServiceImpl(BankAccountDao bankdeo) {
		this.bankdeo=bankdeo;
			
		}

	@Override
	public double checkbalance(long accountId) throws BankAccountNotFoundException{
		
		double balance= bankdeo.getBalance(accountId);

		if(balance>=0)
			return balance;
		throw new BankAccountNotFoundException("account not found");
		
		
	}

	@Override
	public double withdraw(long accountId, double amount) throws LowBalanceException, BankAccountNotFoundException {
		
		double balance=bankdeo.getBalance(accountId);
		if(balance<0)
		{
			throw new BankAccountNotFoundException("Bank account does not exist");
		}
		else if(balance-amount>=0)
		{
			balance=balance-amount;
			bankdeo.updateBalance(accountId, balance);
			bankdeo.commit();

			return balance;
		}
		else
			throw new LowBalanceException("You don't have sufficient fund");
	
	
	}

	@Override
	public double deposit(long accountId, double amount) throws BankAccountNotFoundException {
		
		 double balance=bankdeo.getBalance(accountId);
		 
		 if(balance<0)
			 throw new BankAccountNotFoundException("Bank account not found");
		 
		balance= balance+amount;
		bankdeo.updateBalance(accountId, balance);
		bankdeo.commit();
		System.out.println("amount deposited");
		return balance;
	}

	@Override
	public boolean deleteBankaccount(long accountId) throws BankAccountNotFoundException {

		boolean result= bankdeo.deleteBankaccount(accountId);
		
		if(result)
		{
			bankdeo.commit();
			return result;

		}
		throw new BankAccountNotFoundException("Bank account not exist");
	}


	public double withdrawForFundTransfer(long accountId, double amount) throws LowBalanceException, BankAccountNotFoundException {
		
		double balance=bankdeo.getBalance(accountId);
		if(balance<0)
		{
			throw new BankAccountNotFoundException("Bank account does not exist");
		}
		else if(balance-amount>=0)
		{
			balance=balance-amount;
			bankdeo.updateBalance(accountId, balance);
			return balance;
		}
		else
			throw new LowBalanceException("You don't have sufficient fund");
	
	
	}
	@Override
	public double fundTransfer(long fromAccount, long toAccount, double amount) throws LowBalanceException, BankAccountNotFoundException {
		
		try {
			
			double newBalance=withdrawForFundTransfer(fromAccount, amount);
		deposit(toAccount,amount);
		bankdeo.commit();
		return newBalance;
		}
		catch(LowBalanceException | BankAccountNotFoundException e)
		{
			//logger.error("Exception ",e);
			//Dbutil.rollback();
			throw e;
		}
	}

		
	@Override
	public boolean addNewbankAccount(BankAccount account) {
		System.out.println(account);
	boolean result= bankdeo.addNewbankAccount(account);
	if(result=true)
		bankdeo.commit();
			System.out.println(result);
	return result;

	}

	@Override
	public List<BankAccount> findAllbankAccounts() {
		
		return bankdeo.findAllbankAccounts();
	}

	@Override
	public BankAccount searchAccount(long accountId) throws BankAccountNotFoundException {
		
		BankAccount account=bankdeo.searchAccount(accountId);
		if(account!=null)
			return account;
		throw new BankAccountNotFoundException("Bankaccount not found");
	}

	@Override
	public boolean updateAccount(long accountId, String accountHolderName, String accountType) {
		// TODO Auto-generated method stub
		boolean result= bankdeo.updateAccount(accountId, accountHolderName, accountType);
		if(result)
		 bankdeo.commit();
		
		return result;
		
	}

	
	
}
