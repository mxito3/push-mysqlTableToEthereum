pragma solidity ^0.4.21;
contract pushMes
{
	
	//定义数据库
	
	struct database {
		mapping (string  => table) tables;
	}

  	struct table {
		string  keys;
		data[]  values;
	}

    struct data {
		uint index; 
		string value;	
	}

	
    mapping(string => database)  databases;
    mapping(string => bool)  databaseExist;
    mapping(string => mapping (string => bool))  tableExist;

    event CreateDatabase(string databaseName);
    event CreateTable(string databaseName,string tableName,string keys);

    event PushData(string databaseName,string tableName,uint index);
    
   	function createDatabase(string databaseName) public
	{		
		if(!existSuchDatabase(databaseName))
		{
			databases[databaseName]=database();
			databaseExist[databaseName]=true;
			emit CreateDatabase(databaseName);
		}
	}

	function createTable (string databaseName,string tableName,string keys) public {	
		if(!existSuchDatabase(databaseName))
		{
            createDatabase(databaseName);
		}
		
		if(!exsitSuchTable(databaseName,tableName))
		{
			databases[databaseName].tables[tableName].keys =keys;
			tableExist[databaseName][tableName]=true;
			emit CreateTable(databaseName,tableName,keys);
		}
	}

	function pushData(string databaseName,string tableName,string value,uint index) public
	{
	    
		databases[databaseName].tables[tableName].values.push(data(index,value));
		emit PushData(databaseName,tableName,index);
	}

	function dropTable (string databaseName,string tableName) public
	{
		//清空表内容，避免之前上传过该表
		delete databases[databaseName].tables[tableName].values;
	}
	

	function existSuchDatabase (string databaseName) view public returns(bool)  {
		if(databaseExist[databaseName])
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	function exsitSuchTable(string databaseName,string tableName) view public returns(bool)
	{
		if(tableExist[databaseName][tableName])
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	function getTable(string databaseName,string tableName,uint index) view  public returns(string)
	{
		return databases[databaseName].tables[tableName].values[index].value;
	}

	function getTableKeys (string databaseName,string tableName) public view returns(string)  {
		return databases[databaseName].tables[tableName].keys;
	}
	
	function getTableLength (string databaseName,string tableName) public view returns(uint){
		return databases[databaseName].tables[tableName].values.length;
	}
	
}