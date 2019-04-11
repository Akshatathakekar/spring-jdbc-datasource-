package com.capgemini.bankapp.dao.impl;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import com.capgemini.bankappDao.*;

import com.capgemini.bankapp.exception.BankAccountNotFoundException;

import com.capgemini.bankappDao.BankAccountDao;
import com.capgemini.model.BankAccount;

public class BankAccountDaoImpl implements BankAccountDao {
	
	
	private DataSource dataSource;
	private Connection connection;

	public BankAccountDaoImpl(DataSource dataSource)
	{
		
		this.dataSource=dataSource;
		try
		{		
			connection=dataSource.getConnection();
			connection.setAutoCommit(false);
		}
		catch(Exception e)
		{

		}
		
	}

	

	@Override
	public double getBalance(long accountId)  {
		// TODO Auto-generated method stub
		String query = "select account_balance from bankaccount where account_id=" + accountId;
		double balance = -1;

	
		try (
			PreparedStatement statement = connection.prepareStatement(query);
			ResultSet result = statement.executeQuery();) {
			if(result.next())
			balance = result.getDouble(1);
			

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return balance;

	}

	@Override
	public void updateBalance(long accountId, double newBalance) {
				
		String query="update bankaccount set account_balance=? where account_id=?";
		
		try(PreparedStatement statement=connection.prepareStatement(query) )
		{
			statement.setDouble(1, newBalance);
			statement.setLong(2, accountId);
			int result=statement.executeUpdate();
			System.out.println(result);
			
			//connection commit....
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

	@Override
	public boolean deleteBankaccount(long accountId) {

		String query = "delete from bankaccount where account_id=" + accountId;
		int result;
	
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			result = statement.executeUpdate();

			if (result == 1) {
				return true;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public boolean addNewbankAccount(BankAccount account)  {

		String query="insert into bankaccount(customer_name,account_type,account_balance) values(?,?,?)";
		System.out.println(connection);
		System.out.println(account);
		try(PreparedStatement statement=connection.prepareStatement(query)	)
		{

			System.out.println(account.getAccountHolderName());
			statement.setString(1, account.getAccountHolderName());
			statement.setString(2, account.getAccountType());
			statement.setDouble(3, account.getAccountbalance());
			
			int result=statement.executeUpdate();
			System.out.println(result);
			if(result==1)
			{
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public List<BankAccount> findAllbankAccounts() {

		String query="select * from bankaccount";
		List<BankAccount> accountList=new ArrayList<BankAccount>();
	
		try(PreparedStatement statement=connection.prepareStatement(query))
		{
			ResultSet rs=statement.executeQuery();
		
			while(rs.next())
			{
				long id=rs.getInt(1);
				String name=rs.getString(2);
				String type=rs.getString(3);
				double balance=rs.getDouble(4);
				BankAccount bank=new BankAccount(id,name,type,balance);
				accountList.add(bank);
				
			}
			
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return accountList;
	}

	@Override
	public BankAccount searchAccount(long accountId) {
		String query="select * from bankaccount where account_id="+accountId;
		BankAccount bank=null;
		
		try(PreparedStatement statement=connection.prepareStatement(query))
		{
		ResultSet result=statement.executeQuery();

		result.next();
		long id2=result.getInt(1);
		String name1=result.getString(2);
		String type1=result.getString(3);
		double balance1=result.getDouble(4);
		
		bank=new BankAccount(id2,name1,type1,balance1);
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return bank;
	}

	@Override
	public boolean updateAccount(long accountId, String accountHolderName, String accountType) {
		// TODO Auto-generated method stub
		
		String query="update bankaccount set account_type=?,customer_name=? where account_id="+accountId;
	
		
		try(PreparedStatement statement=connection.prepareStatement(query) )
		{
			statement.setString(1, accountType);
			statement.setString(2, accountHolderName);
			int result=statement.executeUpdate();
		
			System.out.println(result);
			if(result==1)
			{
				return true;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return false;
	}

	@Override
	public void commit()
	{
		try
		{
			if(connection!=null)
				connection.commit();
		}catch(SQLException e)
		{
			//logger.error("SQLException " ,e);
		}
		
	}
	
	@Override
	public void rollback()
	{
		try
		{
			if(connection!=null)
				connection.rollback();
		}catch(SQLException e)
		{
			//logger.error("SQLException " ,e);
		}
		
	}
	
	
}
