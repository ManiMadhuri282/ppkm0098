from pyspark import SparkContext, SparkConf
SparkContext.setSystemProperty('spark.executor.memory', '8g')
SparkContext.setSystemProperty('spark.driver.memory', '8g')
conf = SparkConf().setAppName('task1').setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
rdd = sc.parallelize([1,2,3,4,5,6])
print(sorted(rdd.cartesian(rdd).collect()))
print(sorted(rdd.join(rdd).collect()))


'''
y = sc.parallelize([("a", 1), ("b", 5),("a", 2), ("b", 4),("a", 3), ("b", 2),("a", 1), ("b", 3)]).flatMap()
x = y.groupByKey().mapValues(tuple)
print(x.map(lambda x:x[1]).collect())                      
print(x.cartesian(x).map(lambda x:x[1]).collect())
'''