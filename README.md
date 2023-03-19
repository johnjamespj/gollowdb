# Gollowdb

***Not maintained***

GollowDB is an ***experimental*** key-value store inspired by rocksdb and leveldb. The main objective of the project is to learn how a LSM databases works.

## 📣 Public API

* get(key):         Returns a value from the DB
* set(key, value):  Sets a value to the database
* delete(key):      Deletes a key from the database
* Tail(key):        Iterates through all keys >= key
* Head(key):        Iterates through all keys <= key
* Sub(start, end):  Iterates through all start <= keys <= end
* FirstRow():       Returns First row 
* LastRow():        Returns last row
