# ACID properties: atomicity, consistency, isolation, and durability.
# 
#
#


import os
import uuid

class dblog(object):
    def log(msg):
        print(msg)

    
class simpledbOperation(object):
    pass


class simplePage(object):
    
    def __init__(self) -> None:
        self.pgnum = int()
        self.data = bytearray()
    
   

    
class simpledbAccess(object):
    __data_path = "./simpledb.data"    
    

    def __init__(self, page_size=4000) -> None:
        
        self.page_size = page_size
        
        self.file = os.open(simpledbAccess.__data_path, os.O_RDWR|os.O_CREAT, mode=0o666)
        if not self.file:
            dblog.log("Error: Unable to access database files !")
            exit(1)
        


    def read_page(self, page_num):
        offset = page_num * self.page_size

        self.file.seek(offset)
        data = os.read(self.file)
        if not data:
            return None
        return data

            

    def write_page(self, page):
        offset = page.pgnum * self.page_size

        self.file.seek(offset)
        try:
            os.write(self.file, page.data)
        except IOError as e:
            dblog.log(e)
        

    def __close(self):
        if self.file:
            os.close(self.file)
        del self.file

    



