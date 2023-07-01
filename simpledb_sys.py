import os
import pickle
import uuid

########### Will need to RnD on S3 and name this as mini S3 bucket storage ##############
##### CRUD
# Create
# Insert
# Update
# Delete

class dblog(object):
    def log(msg):
        print(msg)


class BucketDB(object):

    _db_index = "./dbmeta.index"

    _data_path = "./bucketDB"
    
    @classmethod
    def _initialize_db(cls):
        
        cls.index_file = os.path.realpath(f"{cls._data_path}/{cls._db_index}") 

        if not os.path.exists(os.path.realpath(cls._data_path)):
            os.mkdir(os.path.realpath(cls._data_path))
            cls._db_metadata = {}

            # cls.index_file = os.path.realpath(f"{cls._data_path}/{cls._db_index}")
            
            # with open(cls.index_file, 'wb',) as fh:
            #     pickle.dump(cls._db_metadata, fh)

        else:
            
            try:
                with open(cls.index_file, 'rb') as fh:
                    cls._db_metadata  = pickle.load(fh)
                    
            except Exception as ex:
                dblog.log(f"Issue with Index file : {ex}")
                exit(1)

    @classmethod
    def _create(cls, bucket_name):
        
        if bucket_name in cls._db_metadata:
            dblog.log(f"Error: Bucket {bucket_name} exists !")
            return 
        
        db = DataBucket(bucket_name)
        db._create_bucket()

        cls._commit_index(db)

        dblog.log(f"Bucket {db._bucket_name} created successfully !")

        return db
    
    @classmethod
    def _commit_index(cls, bucket):

        # assert os.path.exists(cls.index_file), "Error: Index file Not found.. !"

        with open(cls.index_file, 'wb') as fh:
            cls._db_metadata[bucket._bucket_name] = bucket._bucket
            # fh.write(f"{bucket_name}\n".encode())
            pickle.dump(cls._db_metadata, fh)





########### Data Bucket #######################
class DataBucket(object):

    def __init__(self, bucket_name) -> None:
        super().__init__()
        
        self._bucket_name = bucket_name
        if bucket_name in BucketDB._db_metadata:
            self._bucket = BucketDB._db_metadata[bucket_name]
        
    def _create_bucket(self):

        assert self._bucket_name not in BucketDB._db_metadata, "Error: Duplicate Bucket {self._bucket_name} cannot be created !"

        try:
            self._bucket = os.path.realpath(f"{BucketDB._data_path}/{uuid.uuid4().hex}.bkt")
            os.open(f"{self._bucket}", flags=os.O_CREAT)
        except Exception as e:
            dblog.log(f"Error: Cannot create data bucket {e}")
        

    def _load(self, data_buffer):
        assert self._bucket_name in BucketDB._db_metadata, dblog.log("Error: Bucket dose not exists !")

        bucket_hd = os.open(f"{self._bucket}", os.O_WRONLY | os.O_APPEND | os.O_BINARY, mode=0o666)

        chunk = data_buffer[0:100]
        chunk_pos = 100
        while True:
            if not chunk:
                break
            print(chunk)
            
            os.write(bucket_hd, chunk.encode().hex().encode())
                                 
            chunk = data_buffer[chunk_pos: chunk_pos + 100]
            chunk_pos += 100

        os.close(bucket_hd)
        
    def _read(self):
            assert self._bucket_name in BucketDB._db_metadata, dblog.log("Error: Bucket dose not exists !")

            fh = os.open(f"{self._bucket}", os.O_RDONLY | os.O_BINARY, mode=0o666)
            buffer = ''
            while True:
                buffer = os.read(fh, 1024)
                if not buffer:
                     os.close(fh)
                     break
                print(bytes.fromhex(buffer.decode()))

            
                
        



################### Test ########################
BucketDB._initialize_db()

# BucketDB._create("Test2")

bkt = DataBucket("Test")

# bkt = BucketDB._create("Test2")

# print(len(BucketDB._db_metadata))

# bkt = DataBucket(bucket_name="Test2")
# print(bkt._bucket)
# bkt._load("Id,Name,Age")
# bkt._load("\n1,Ajay,40\n" * 10)

bkt._read()




