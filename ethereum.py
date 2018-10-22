# -*- coding:utf-8 -*-
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware
import time 
class Ethereum(object):
    def __init__(self,coinbase_password,contractAddress,contractAbi):
        self.web3 = Web3(HTTPProvider('http://localhost:8545'))
        self.web3.middleware_stack.inject(geth_poa_middleware, layer=0)
        assert self.web3.isConnected(),'connect fail'
        self.ourAddress = self.web3.eth.accounts[0]
        self.ourPassword = coinbase_password
        self.contractAddress = self.web3.toChecksumAddress(contractAddress)
        self.abi=contractAbi
        self.contract=self.getContract()
    def getContract(self):
        contract = self.web3.eth.contract(address=self.contractAddress, abi=self.abi)
        return contract

    def createDatabase(self,databaseName):
        unlockResult = self.web3.personal.unlockAccount(self.ourAddress, self.ourPassword)
        if (unlockResult):
            hash = self.contract.functions.createDatabase(databaseName).transact({'from': self.ourAddress})
            if (hash):
                print("创建数据库"+databaseName+"交易发起成功hash值是" + self.web3.toHex(hash))
                self.web3.personal.lockAccount(self.ourAddress)
                self.watingMined(hash)

    def createTable(self,databaseName,tableName,keys):
        unlockResult = self.web3.personal.unlockAccount(self.ourAddress, self.ourPassword)
        if (unlockResult):
            hash = self.contract.functions.createTable(databaseName,tableName,keys).transact({'from': self.ourAddress})
            if (hash):
                print("在数据库"+databaseName+"中创建表  "+tableName+"　　的交易发起成功hash值是" + self.web3.toHex(hash))
                self.web3.personal.lockAccount(self.ourAddress)
                self.watingMined(hash)

    def pushData(self,databaseName,tableName,value,index):
        unlockResult = self.web3.personal.unlockAccount(self.ourAddress, self.ourPassword)
        if (unlockResult):
            hash = self.contract.functions.pushData(databaseName,tableName,value,index).transact({'from': self.ourAddress})
            if (hash):
                print("在数据库"+databaseName+"的表  "+tableName+"　　中上传数据  "+value+" 的交易成功发起hash值是" + self.web3.toHex(hash))
                self.web3.personal.lockAccount(self.ourAddress)

    def watingMined(self,hash):
        print("交易 "+self.web3.toHex(hash)+"  打包中,请稍后...")
        needWaitTime=3
        while self.web3.eth.getTransactionReceipt(hash) is None:
            time.sleep(needWaitTime)
        # callback(self.web3.toHex(hash)+"  交易打包成功")
        print("交易 "+self.web3.toHex(hash)+"  打包成功")

    def getTableLength(self,databaseName,tableName):
        return self.self.contract.functions.getTableLength(databaseName,tableName).call()

    def getTable(self,databaseName,tableName,index):
        return self.contract.functions.getTable(databaseName,tableName,index).call()
    def exsitSuchTable(self,databaseName,tableName):
        return self.contract.functions.exsitSuchTable(databaseName,tableName).call()
    def existSuchDatabase(self,databaseName):
        return self.contract.functions.existSuchDatabase(databaseName).call()
    def getTableLength(self,databaseName,tableName):
        return self.contract.functions.getTableLength(databaseName,tableName).call()
    def  getTable(self,databaseName,tableName,index):
        return self.contract.functions.getTable(databaseName,tableName,index).call()
    def getTableKeys(self,databaseName,tableName):
        return self.contract.functions.getTableKeys(databaseName,tableName).call()
    
if __name__ =="__main__":
    coinbasePassword='123456'
    contractAddress='0xaC83B4384f600F5aF2C287b8Ba56d50d5F19d224'
    contractAbi='[{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"},{"indexed":false,"name":"tableName","type":"string"},{"indexed":false,"name":"index","type":"uint256"}],"name":"PushData","type":"event"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"}],"name":"createDatabase","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"},{"indexed":false,"name":"tableName","type":"string"},{"indexed":false,"name":"keys","type":"string"}],"name":"CreateTable","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"databaseName","type":"string"}],"name":"CreateDatabase","type":"event"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"keys","type":"string"}],"name":"createTable","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"value","type":"string"},{"name":"index","type":"uint256"}],"name":"pushData","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"}],"name":"existSuchDatabase","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"exsitSuchTable","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"},{"name":"index","type":"uint256"}],"name":"getTable","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"getTableKeys","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"databaseName","type":"string"},{"name":"tableName","type":"string"}],"name":"getTableLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'
    geth=Ethereum(coinbasePassword,contractAddress,contractAbi)
    db='test2'
    table='test1'
    value="hello world2"
    index=geth.getTableLength(db,table)
    # print(geth.getContract().__dict__)
    # geth.createDatabase('test')
    # geth.createTable('test','test1','key1 key2')
    print(geth.exsitSuchTable(db,table))
    # print(geth.existSuchDatabase(db))
    # geth.pushData(db,table,value,index)
    # print(geth.getTable(db,table,1))