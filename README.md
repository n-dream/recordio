# RecordIO for Google App Engine

## What are RecordIOs

A RecordIO consists of a sorted list of key value pairs that can be
persistently stored in Google App Engine. RecordIOs can hold terabytes of data.

- Extremely efficient in writing many small entries.
- Extremely powerful because the size of entry can exceed 1MB, which means you
  can store bigger objects in RecordIOs than in App Engines Datastore.

### What RecordIOs are good for?

In case you want to write a lot of key value pairs to datastore, RecordIOs are
for you. RecordIOs use much fewer datastore operations than if you would create
a single model entity for every key value pair.

### Why is it better than datastore?

#### Cheaper and faster
Multiple key value pairs are combined and compressed into a one datastore entry
which makes writing many key value pairs more efficient, especially if you can
write asynchronously. In this case, writes are added to a queue and after a
pre-set amount of time, all writes are applied at the same time and combined
whenever possible. This minimizes datastore operations and therefor costs.

#### Store objects of arbitrary size
RecordIO can store objects of any size. If insert an object that is bigger than
1MB it gets split into smaller chunks and distributed over multiple datastore
entries, all automagically.

### Where are RecordIOs stored?

The RecordIOs end up in datastore as sharded data. Before asynchronous writes
are applied to the shards in datastore, they are queued in a normal TaskQueue.

## Installation

Please add the following lines to your ```app.yaml```

```
handlers:
- url: /recordio/.*
  login: admin
  script: recordio.recordio_handler.application

```

Please add the following lines to your ```queue.yaml```

```
total_storage_limit: 1T

queue:
- name: recordio-writer
  rate: 100/s
  bucket_size: 100
  max_concurrent_requests: 1000
- name: recordio-queue
  mode: pull
```

It is advised that you set the ```total_storage_limit``` to a big value if you
are going to write a lot of data to RecordIOs asynchronously.

## Usage

### An example:

#### Writing
```python
from recordio import RecordIOWriter

writer = RecordIOWriter("HelloWorldRecordIO")
writer.create()
writer.insert("HelloString", "SomeString")
writer.insert("HelloJSON", {"some": "json"})
writer.insert("HelloPickle", somePickleableObject)

# Write asynchronously and batch with other writes to same recordio ...
writer.commit_async()

# ... or write synchronously
writer.commit_sync()
```

#### Reading
All data:
```python
from recordio import RecordIOReader

reader = RecordIOReader("HelloWorldRecordIO")

for key, value in reader:
  print key, value
```
Will print:
```
HelloJSON {"some": "json"}
HelloPickle somePickleableObject
HelloString SomeString
```

A certain range:
```python
for key, value in reader.read(start_key="HelloP", end_key="HelloZ"):
  print key, value
```
Will print:
```
HelloPickle somePickleableObject
HelloString SomeString
```

A single element:
```python
print reader["HelloString"]
```
Will print:
```
SomeString
```


### Writing to a RecordIO

For all write operations you will use the ```RecordIOWriter``` class.


#### RecordIOWriter

Constructor

```RecordIOWriter(name)```

Arguments:

- name: The name of your RecordIO

#### Creating a RecordIO

Before you can write to a RecordIO, you need to create the RecordIO.
If a RecordIO with this name already exists, it is untouched.

```create(self, compressed=True, pre_split=[])```

Arguments:

- compressed: Boolean if the data in the RecordIO should be gzipped.
- pre_split: An optional list of keys to that should be used to pre-split
             the internal data shards. This is only makes sense if you are
             going to write a lot of data and you already know the key range
             of the data and roughly how many entries fit into one shard.

Returns True, if the RecordIO didn't exist before.

#### Inserting into a RecordIO

Assigns a value to a certain key. Overwrites existing values with the same key.

```insert(self, key, value)```

Arguments:

- key: Must be a string and must not be longer than 64 characters.
- value: Values can be of any type that is pickeable (anything you can put in
         memcache). Values can have arbitrary size (There is no size limit like
         normal Datastore entries have).

#### Removing elements from a RecordIO

```remove(self, key)```

Arguments:

- key: A key of a previously inserted value. If this key does not exist, no
       exception is thrown.

#### Applying the changes

```commit_sync()``` writes the changes synchronously to the RecordIO.

```commit_sync(self, retries=32, retry_timeout=1)```

Arguments:

- retries: How many times a commit_sync should be retried in case of datastore
           collisions.
- retry_timeout: The amount of second to wait before the next retry.

```commit_async()``` writes the changes asynchronously to
the RecordIO and automatically batches other pending writes to the same
RecordIO (Cheaper and more efficient than synchronous commits).

```commit_async(self, write_every_n_seconds=300)```

Arguments:

- write_every_n_seconds: Writes the changes after this amount of seconds to
                         the RecordIO.

#### Deleting a RecordIO

```delete(self)```

Deletes a RecordIO. Modifying RecordIOs or applying queued writes may result
in errors during deletions.

### Reading from a RecordIO

For all read operations you will use the ```RecordIOReader``` class.


#### RecordIOReader constructor

```RecordIOReader(name)```

Arguments:

- name: The name of your RecordIO

#### Reading all entries:

```for key, value in RecordIOReader('name')```

Returns a tuple of ```key, value```.

Reads sequentially through the sorted RecordIO and yields ```key, value```.
The values are already parsed (have the same type like when they were
inserted).

#### Reading through a range of entries:

```read(self, start_key="", end_key=None, limit=None)```

Arguments:

- start_key: The starting key.
- end_key: The ending key (exclusive).
- limit: Integer, the maximum amount of entries to be read.

Returns a tuple of ```key, value```.

#### Reading a single element:

Returns a single element. If possible scan through ranges instead.

```for x in RecordIOReader('name')['key']```

Returns the value for a certain key.

## RecordIO Handlers:

### RecordIOViewer
Is available at ```/recordio/```.

### RecordIOWriter
In case you have asynchronous pending writes that somehow never get applied
(this should never happen unless you accidentally emptied worker queues)
you can go to ```/recordio/write``` to schedule the writers again.

### RecordIOLoadtest
If you want to loadtest RecordIOs and find out what fine tuning is best for
you, mess around in ```/recordio/loadtest```.