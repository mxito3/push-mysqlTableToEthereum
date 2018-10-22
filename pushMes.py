# -*- coding:utf-8 -*-
from sql import Sql
from ethereum import Ethereum
import json
import threading
class pushMes(object):
    """docstring for pushMes"""
    def __init__(self,dbName, dbPassword, tableName,coinbasePassword,contractAddress,contractAbi):
        super(pushMes, self).__init__()
        self.sql = Sql("localhost", 3306, 'root', dbPassword, dbName)
        self.tableNeedRead = tableName
        self.databaseName=dbName
        self.geth=Ethereum(coinbasePassword,contractAddress,contractAbi)
        # self.dataLength=
    def getData(self):
        self.sql.connect()
        command='select * from %s'%self.tableNeedRead
        result=self.sql.extractSql(command)
        self.sql.close()
        datas=[]
        keys=[]
        keyJson=json.loads(self.getKeys())
        # 获取字段
        for item in keyJson:
            keys.append(keyJson.get(item))
        #处理数据
        for i,item in enumerate(result):
            data={}
            for i,perValue in enumerate(item):
                data[keys[i]]=perValue
            # data[i]=
            datas.append(json.dumps(data,ensure_ascii=False))

        return datas            
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

    def waitMined(self,index):
        print("第　"+str(index+1)+"　条数据上传完成")
        self.uploadedLength=self.uploadedLength+1
        if self.uploadedLength == self.needUploadLength:
            print("全部上传完成\n从区块链获取数据库"+self.databaseName+"获取表"+self.tableNeedRead+"...")
            for data in self.getFromBlockChain(self.databaseName,self.tableNeedRead):
                print(data)
        
    def pushToBlockChain(self,needUploadDB,needUploadTable):
        print("获取本地数据库数据 ...")
        keys=self.getKeys()
        values=self.getData()
        self.needUploadLength=len(values)
        self.uploadedLength=0

        print("本地数据库获取完毕")
        if not self.geth.existSuchDatabase(needUploadDB):
            print("在区块链创建数据库  "+needUploadDB+" ...")
            #创建数据库
            self.geth.createDatabase(needUploadDB)
            print("在区块链创建数据库  "+needUploadDB+" 完毕")
        else:
            print("数据库  "+needUploadDB+"在区块链已存在")            
        if not self.geth.exsitSuchTable(needUploadDB,needUploadTable):

            print("在区块链中创建表　"+needUploadTable+"　...")
            self.geth.createTable(needUploadDB,needUploadTable,keys)
            print("在区块链创建表　"+needUploadTable+"　成功")

            
        else:
            print("区块链数据库　"+needUploadDB+"　中已存在表 "+needUploadTable)            
        print("准备上传数据 ...")
        self.geth.pushData(needUploadDB,needUploadTable,values,self.waitMined)



    def getFromBlockChain(self,needGetDB,needGetTable):
        if not geth.exsitSuchTable(needGetDB,needGetTable):
            print("区块中不存在这样的表")
            return
        else:
            length=geth.getTableLength(needGetDB,needGetTable)
            result=[]
            for index in range(length):
                result.append(geth.getTable(needGetDB,needGetTable,index))
            return result



if __name__ == '__main__':
    dbName='videos'
    tableName='requestQuanitity'
    dbPassword='domore0325'
    coinbasePassword='123456'
    contractAddress='0x0fc4FA5aa4d664b49B58E95a997671Ebbb44Db48'
    contractAbi='[{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"},{"indexed":false,"name":"tableName","type":"string"},{"indexed":false,"name":"index","type":"uint256"}],"name":"PushData","type":"event"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"}],"name":"createDatabase","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"},{"indexed":false,"name":"tableName","type":"string"},{"indexed":false,"name":"keys","type":"string"}],"name":"CreateTable","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"}],"name":"CreateDatabase","type":"event"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"keys","type":"string"}],"name":"createTable","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"value","type":"string"},{"name":"index","type":"uint256"}],"name":"pushData","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"}],"name":"existSuchDatabase","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"exsitSuchTable","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"index","type":"uint256"}],"name":"getTable","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"getTableKeys","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"getTableLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'
    geth=Ethereum(coinbasePassword,contractAddress,contractAbi)
    test=pushMes(dbName,dbPassword,tableName,coinbasePassword,contractAddress,contractAbi)
    test.pushToBlockChain(dbName,tableName)
   




