# -*- coding:utf-8 -*-
from sql import Sql
from ethereum import Ethereum
import json

class pushMes(object):
    """docstring for pushMes"""
    def __init__(self,dbName, dbPassword, tableName,coinbasePassword,contractAddress,contractAbi):
        super(pushMes, self).__init__()
        self.sql = Sql("localhost", 3306, 'root', dbPassword, dbName)
        self.tableNeedRead = tableName
        self.databaseName=dbName
        self.geth=Ethereum(coinbasePassword,contractAddress,contractAbi)
    def getData(self):
        self.sql.connect()
        command='select * from %s'%self.tableNeedRead
        result=self.sql.extractSql(command)
        self.sql.close()
        datas={}
        for i,item in enumerate(result):
        	datas[i]=item
        return json.dumps(datas,ensure_ascii=False)

    def getKeys(self):
        self.sql.connect()
        command="select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s' and table_schema = '%s'"%(self.tableNeedRead,self.databaseName)
        result=self.sql.extractSql(command)
        self.sql.close()
        keys={}
        for i,item in enumerate(result):
            keys[i]=item[0]
        # print(keys)            
        return json.dumps(keys,ensure_ascii=False)
    def waitMined():
        # print(result)
        pass

    def pushToBlockChain(self,needUploadDB,needUploadTable):
        print("获取本地数据库数据 ...")
        keys=self.getKeys()
        vlaues=self.getData()
        print("本地数据库获取完毕")
        if not self.geth.existSuchDatabase(needUploadDB):
            print("在区块链创建数据库  "+needUploadDB+" ...")
            # pri?nt(keys)
            # print(vlaues)
            #创建数据库
            self.geth.createDatabase(needUploadDB)
            print("在区块链创建数据库  "+needUploadDB+" 完毕")
        else:
            print("数据库  "+needUploadDB+"在区块链已存在")            
        if not self.geth.exsitSuchTable(needUploadDB,needUploadTable):

            print("在区块链创建表　"+needUploadTable+"　...")
            self.geth.createTable(needUploadDB,needUploadTable,keys)
            print("在区块链创建表　"+needUploadTable+"　成功")
            print(self.geth.getTableKeys(needUploadDB,needUploadTable))

            #清空表的内容
        else:
            print("区块链数据库　"+needUploadDB+"　中已存在表 "+needUploadTable)            
        

        print("准备上传数据 ...")



if __name__ == '__main__':
    dbName='videos'
    dbPassword='domore0325'
    tableName='videoMap'
    coinbasePassword='123456'
    contractAddress='0xaC83B4384f600F5aF2C287b8Ba56d50d5F19d224'
    contractAbi='[{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"},{"indexed":false,"name":"tableName","type":"string"},{"indexed":false,"name":"index","type":"uint256"}],"name":"PushData","type":"event"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"}],"name":"createDatabase","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"},{"indexed":false,"name":"tableName","type":"string"},{"indexed":false,"name":"keys","type":"string"}],"name":"CreateTable","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"}],"name":"CreateDatabase","type":"event"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"keys","type":"string"}],"name":"createTable","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"value","type":"string"},{"name":"index","type":"uint256"}],"name":"pushData","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"}],"name":"existSuchDatabase","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"exsitSuchTable","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"index","type":"uint256"}],"name":"getTable","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"getTableKeys","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"getTableLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'
    db='test2'
    table='test1'
    value="hello world2"
    # geth=Ethereum(coinbasePassword,contractAddress,contractAbi)
    test=pushMes(dbName,dbPassword,tableName,coinbasePassword,contractAddress,contractAbi)
    # print(test.getData())
    # print(test.getKeys())
    # index=geth.getTableLength(db,table)
    # print(geth.getTable(db,table,1))
    test.pushToBlockChain(db,table)




