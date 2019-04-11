package com.capgemini.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.List;

//import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.capgemini.bankapp.exception.BankAccountNotFoundException;
import com.capgemini.bankapp.exception.LowBalanceException;
import com.capgemini.bankapp.service.impl.BankAccountServiceImpl;
import com.capgemini.bankapp.service.*;
import com.capgemini.model.*;

public class BankaccountClient {

	//static final Logger logger=Logger.getLogger(BankaccountClient.class);

	public static void main(String[] args) throws BankAccountNotFoundException {

		int choice;

		ApplicationContext context=new ClassPathXmlApplicationContext("context.xml");
		BankAccountService service=(BankAccountService)context.getBean("bankAccountServiceImpl");


		try( BufferedReader reader=new BufferedReader(new InputStreamReader(System.in)) )
		{
			while(true)
			{
				System.out.println("1.Add Bank account \n2.Withdraw \n3.Deposit \n4.Fund Transfer");
				System.out.println("5.Delete bank account \n6.Check Balance ");
				System.out.println("7.Display bank account details\n8.Update details  \n9.Search bank Account \n10.Exit\n");
				
				System.out.println("Enter your choice");
				choice = Integer.parseInt(reader.readLine());
				switch(choice)
				{
				
				case 1: System.out.println("Enter your name");
						String accountHolderName=reader.readLine();
						System.out.println("Enter account type");
						String accountType=reader.readLine();
						System.out.println("Enter account balance");
						double balance=Double.parseDouble(reader.readLine());
						BankAccount bank=
						//new BankAccount(accountHolderName,accountType,balance);
						(BankAccount)context.getBean("bankacc");
						bank.setAccountHolderName(accountHolderName);
						bank.setAccountbalance(balance);
						bank.setAccountType(accountType);
						
						System.out.println(bank.toString());
						if(service.addNewbankAccount(bank))
						{
							System.out.println("Account created successfully");
						}
						else
						{
							System.out.println("Failed to create new account ");
						}
					
						break;
					
			     case 2: System.out.println("Enter your accountId");
						long accountId=Integer.parseInt(reader.readLine());
						System.out.println("Enter amount");
						double amount=Double.parseDouble(reader.readLine());
					
						try {
							double balance1=service.withdraw(accountId, amount);
							System.out.println(balance1);

						} 
						catch (LowBalanceException e) {
							//System.out.println(e.getMessage());
							//logger.error("Withdraw failed :",e);

						}
					
						break;

				 case 3: System.out.println("Enter your accountId");
						long accountId2=Integer.parseInt(reader.readLine());
						System.out.println("Enter amount");
						double amount2=Double.parseDouble(reader.readLine());
						try {
							service.deposit(accountId2, amount2);
						}
						catch(BankAccountNotFoundException e)
						{
						//logger.error("account not found ",e);
						}
						
						break;

				case 4:System.out.println("Enter your account id ");
						long fromAccount=Integer.parseInt(reader.readLine());
						System.out.println("Enter account id in which  you want to transfer amount");
						long toAccount=Integer.parseInt(reader.readLine());

						System.out.println("Enter amount");
						double NewAmount=Double.parseDouble(reader.readLine());
						
						try {
						double balance2=service.fundTransfer(fromAccount, toAccount, NewAmount);
							System.out.println(balance2);
						} catch (LowBalanceException e) {
							System.out.println(e.getMessage());
						}catch(BankAccountNotFoundException e) {
							System.out.println(e.getMessage());
						}
						
						break;

				case 5:   System.out.println("Enter your accountId");
						long accId=Integer.parseInt(reader.readLine());
						boolean result=service.deleteBankaccount(accId);
						System.out.println(result);
						break;

				case 6:
						System.out.println("Enter your account id");
						long accId3=Integer.parseInt(reader.readLine());

						 double balanceNew=service.checkbalance(accId3);
						 System.out.println(balanceNew);
						 break;

				case 7:List<BankAccount> accountList=service.findAllbankAccounts();
						for(BankAccount a:accountList)
						{
							System.out.println(a);
							
						}
					
					
						break;
						
				case 8: System.out.println("Enter accountId");
						long accId31=Integer.parseInt(reader.readLine());
						System.out.println("enter name");
						String accountHolderName2=reader.readLine();

						System.out.println("Enter account Type");
						String accountType1=reader.readLine();
						
						boolean resultNew=service.updateAccount(accId31, accountHolderName2, accountType1);
							if(resultNew==true)
							{
								System.out.println("updated successfully");
							}
						break;
				
				case 9:System.out.println("Enter account id");
						long accId2=Integer.parseInt(reader.readLine());
						try {
						BankAccount bankAccount=service.searchAccount(accId2);
						System.out.println(bankAccount);
						}
						catch(BankAccountNotFoundException e)
						{
							//logger.error("bank account not found");
						}
						break;

				case 10:System.out.println("Thanks for banking");
						System.exit(0);
				
				
				}
			}
			
		} catch (IOException e) {
			//	e.printStackTrace();
			//logger.error("Exception :",e);
		}
		
	}
	
	
	
}
