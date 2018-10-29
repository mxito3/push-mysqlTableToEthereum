# -*- coding:utf-8 -*-
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware
import time 
import threading
class Ethereum(object):
    def __init__(self,coinbase_password,contractAddress,contractAbi):
        self.web3 = Web3(HTTPProvider('http://localhost:8545'))
        self.web3.middleware_stack.inject(geth_poa_middleware, layer=0)
        assert self.web3.isConnected(),'connect fail 请打开geth'
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

    def pushData(self,databaseName,tableName,values,callback):
        for index,value in enumerate(values):    
            unlockResult = self.web3.personal.unlockAccount(self.ourAddress, self.ourPassword)
            if (unlockResult):
                hash = self.contract.functions.pushData(databaseName,tableName,value,index).transact({'from': self.ourAddress})
                if (hash):
                    print("在数据库"+databaseName+"的表  "+tableName+"　　中上传数据  "+value+" 的交易成功发起hash值是" + self.web3.toHex(hash))
                    self.web3.personal.lockAccount(self.ourAddress)
                    #
                    waitingUpload(self,hash,index,callback).start()
    def dropTable(self,databaseName,tableName):
        unlockResult = self.web3.personal.unlockAccount(self.ourAddress, self.ourPassword)
        if (unlockResult):
            hash = self.contract.functions.dropTable(databaseName,tableName).transact({'from': self.ourAddress})
            if (hash):
                print("删除已经存在的表"+tableName+"　　的交易发起成功,hash值是" + self.web3.toHex(hash))
                self.web3.personal.lockAccount(self.ourAddress)
                self.watingMined(hash)

    def watingMined(self,hash,type=None,index=None,callback=None):
        print("交易 "+self.web3.toHex(hash)+"  打包中,请稍后...")
        needWaitTime=3
        while self.web3.eth.getTransactionReceipt(hash) is None:
            time.sleep(needWaitTime)
        # callback(self.web3.toHex(hash)+"  交易打包成功")
        print("交易 "+self.web3.toHex(hash)+"  打包成功")
        # print(index)
        # print(not callback is None)
        if not callback is None and not index is None:
            callback(index)

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


class waitingUpload(threading.Thread):
    def __init__(self,Ethereum,hash,index,callback):
        threading.Thread.__init__(self)
        self.hash=hash
        self.index=index
        self.geth=Ethereum
        self.callback=callback
    def run(self): 
        self.geth.watingMined(self.hash,index=self.index,callback=self.callback)
