# Common patterns in exams

## MapReduce

- ### Count how many

   (key, 1), iterate over val and sum

- ### Every from one set

   (key, val), iterate over val and check for change

- ### More A than B

   (key, 1/-1), iterate over val and sum

- ### At least

   (key, null), see size of val

- ### Max

   (2 job), local sum and then aggregate result selecting max

## SparkRDD

- ### Window of n

   flatMapToPair() that emits an array of n tuples -> (index, val), ... , (index-n-1, val). Then reduceByKey()

- ### > or < to threshold

   compute threshold and then filter

- ### A < or > B

   same as MapReduce, mapToPair() with (key, -+1) and compute diff

- ### A and B < or > threshold

   mapToPair() -> if A: (key, (1, 0)), if B: (key, (0, 1)). Then reduce and filter
