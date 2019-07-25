# [02] RDD

# 2.1 RDD

## ~~2.1.1 들어가기에 앞서~~

## 2.1.2 스파크컨텍스트 생성

---

- Spark 컨텍스트란? Spark 애플리케이션과 클러스터의 연결을 관리하는 객체
    - 스파크 컨텍스트 생성 시 스파크 동작에 필요한 설정 정보를 지정할 수 있음
    - 클러스터 마스터 정보와 애플리케이션 이름은 필수 정보
    - `local[*]` 안의 내용은 사용할 스레드의 개수를 의미

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RDDCreateSample");
        JavaSparkContext sc = new JavaSparkContext(conf);

## 2.1.3 RDD 생성

---

1. 드라이버 프로그램의 컬렉션 객체를 이용

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));

2. 파일이나 데이터베이스의 외부 데이터를 사용

        JavaRDD<String> rdd1 = sc.textFile("<spark_home_dir>/README.md");

## ~~2.1.4 RDD 기본 액션 → 2.1.6 RDD 액션~~

## 2.1.5 RDD 트랜스포메이션

---

### **map과 관련 연산**

- 2.1.5.1 map

        public static void doMap(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        	// Java7
        	JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {
        		@Override
        		public Integer call(Integer v1) throws Exception {
        			return v1 + 1;
        		}
        	});
        	// Java8 Lambda
        	JavaRDD<Integer> rdd3 = rdd1.map((Integer v1) -> v1 + 1);
        	System.out.println(StringUtils.join(rdd2.collect(), ", "));
        }

- 2.1.5.2 flatMap

        public static void doFlatMap(JavaSparkContext sc) {
        	List<String> data = new ArrayList();
        	data.add("apple,orange");
        	data.add("grape,apple,mango");
        	data.add("blueberry,tomato,orange");
        	JavaRDD<String> rdd1 = sc.parallelize(data);
        	JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
        		@Override
        		public Iterator<String> call(String t) throws Exception {
        			return Arrays.asList(t.split(",")).iterator();
        		}
        	});
        	// Java8 Lambda
        	JavaRDD<String> rdd3 = rdd1.flatMap((String t) -> Arrays.asList(t.split(",")).iterator());
        	System.out.println(rdd2.collect());
        }

- 2.1.5.3 mapPartitions

        public static void doMapPartitions(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        	JavaRDD<Integer> rdd2 = rdd1.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
        		public Iterator<Integer> call(Iterator<Integer> numbers) throws Exception {
        			System.out.println("DB연결 !!!");
        			List<Integer> result = new ArrayList<>();
        			while (numbers.hasNext()) {
        				result.add(numbers.next() + 1);
        			}
        			return result.iterator();
        		}
        		;
        	});
        	// Java8 Lambda
        	JavaRDD<Integer> rdd3 = rdd1.mapPartitions((Iterator<Integer> numbers) -> {
        		System.out.println("DB연결 !!!");
        		List<Integer> result = new ArrayList<>();
        		numbers.forEachRemaining(i -> result.add(i + 1));
        		return result.iterator();
        	});
        	System.out.println(rdd2.collect());
        }

- 2.1.5.4 mapPartitionsWithIndex

        public static void doMapPartitionsWithIndex(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        	JavaRDD<Integer> rdd2 = rdd1
        			.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
        				@Override
        				public Iterator<Integer> call(Integer idx, Iterator<Integer> numbers) throws Exception {
        					List<Integer> result = new ArrayList<>();
        					if (idx == 1) {
        						while (numbers.hasNext()) {
        							result.add(numbers.next() + 1);
        						}
        					}
        					return result.iterator();
        				}
        			}, true);
        	// Java8 Lambda
        	JavaRDD<Integer> rdd3 = rdd1.mapPartitionsWithIndex((Integer idx, Iterator<Integer> numbers) -> {
        		List<Integer> result = new ArrayList<>();
        		if (idx == 1)
        			numbers.forEachRemaining(i -> result.add(i + 1));
        		return result.iterator();
        	}, true);
        	System.out.println(rdd2.collect());
        }

- 2.1.5.5 mapValues

        public static void doMapValues(JavaSparkContext sc) {
        	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        	JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
        		@Override
        		public Tuple2<String, Integer> call(String t) throws Exception {
        			return new Tuple2(t, 1);
        		}
        	});
        	JavaPairRDD<String, Integer> rdd3 = rdd2.mapValues(new Function<Integer, Integer>() {
        		@Override
        		public Integer call(Integer v1) throws Exception {
        			return v1 + 1;
        		}
        	});
        	// Java8 Lambda
        	JavaPairRDD<String, Integer> rdd4 = rdd1.mapToPair((String t) -> new Tuple2<String, Integer>(t, 1))
        			.mapValues((Integer v1) -> v1 + 1);
        	System.out.println(rdd3.collect());
        }

- 2.1.5.6 flatMapValues

        public static void doFlatMapValues(JavaSparkContext sc) {
        	List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2(1, "a,b"), new Tuple2(2, "a,c"),
        			new Tuple2(1, "d,e"));
        	JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(data);
        	// Java7
        	JavaPairRDD<Integer, String> rdd2 = rdd1.flatMapValues(new Function<String, Iterable<String>>() {
        		@Override
        		public Iterable<String> call(String v1) throws Exception {
        			return Arrays.asList(v1.split(","));
        		}
        	});
        	// Java8 Lambda
        	JavaPairRDD<Integer, String> rdd3 = rdd1.flatMapValues((String v1) -> Arrays.asList(v1.split(",")));
        	System.out.println(rdd2.collect());
        }

### **그룹 관련 연산**

- 2.1.5.7 zip

        public static void doZip(JavaSparkContext sc) {
        	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        	JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3));
        	JavaPairRDD<String, Integer> result = rdd1.zip(rdd2);
        	System.out.println(result.collect());
        }

- 2.1.5.8 zipPartitions

        public static void doZipPartitions(JavaSparkContext sc) {
        	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"), 3);
        	JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2), 3);
        	// Java7
        	JavaRDD<String> rdd3 = rdd1.zipPartitions(rdd2,
        			new FlatMapFunction2<Iterator<String>, Iterator<Integer>, String>() {
        				@Override
        				public Iterator<String> call(Iterator<String> t1, Iterator<Integer> t2) throws Exception {
        					List<String> list = new ArrayList<>();
        					while (t1.hasNext()) {
        						if (t2.hasNext()) {
        							list.add(t1.next() + t2.next());
        						} else {
        							list.add(t1.next());
        						}
        					}
        					return list.iterator();
        				}
        			});
        	// Java8 Lambda
        	JavaRDD<String> rdd4 = rdd1.zipPartitions(rdd2, (Iterator<String> t1, Iterator<Integer> t2) -> {
        		List<String> list = new ArrayList<>();
        		t1.forEachRemaining((String s) -> {
        			if (t2.hasNext()) {
        				list.add(s + t2.next());
        			} else {
        				list.add(s);
        			}
        		});
        		return list.iterator();
        	});
        	System.out.println(rdd4.collect());
        }

- 2.1.5.9 groupBy

        public static void doGroupBy(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        	// Java7
        	JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupBy(new Function<Integer, String>() {
        		@Override
        		public String call(Integer v1) throws Exception {
        			return (v1 % 2 == 0) ? "even" : "odd";
        		}
        	});
        	// Java8 Lambda
        	JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd1.groupBy((Integer v1) -> (v1 % 2 == 0) ? "even" : "odd");
        	System.out.println(rdd2.collect());
        }

- 2.1.5.10 groupByKey

        public static void doGroupByKey(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("c", 1),
        			new Tuple2("b", 1), new Tuple2("c", 1));
        	JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);
        	JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupByKey();
        	System.out.println(rdd2.collect());
        }

- 2.1.5.11 cogroup

        public static void doCogroup(JavaSparkContext sc) {
        	List<Tuple2<String, String>> data1 = Arrays.asList(new Tuple2("k1", "v1"), new Tuple2("k2", "v2"),
        			new Tuple2("k1", "v3"));
        	List<Tuple2<String, String>> data2 = Arrays.asList(new Tuple2("k1", "v4"));
        	JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(data1);
        	JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(data2);
        	JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> result = rdd1.<String>cogroup(rdd2);
        	System.out.println(result.collect());
        }

### **집합 관련 연산**

- 2.1.5.12 distinct

        public static void doDistinct(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3));
        	JavaRDD<Integer> result = rdd.distinct();
        	System.out.println(result.collect());
        }

- 2.1.5.13 cartesian

        public static void doCartesian(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
        	JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "c"));
        	JavaPairRDD<Integer, String> result = rdd1.cartesian(rdd2);
        	System.out.println(result.collect());
        }

- 2.1.5.14 subtract

        public static void doSubtract(JavaSparkContext sc) {
        	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
        	JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("d", "e"));
        	JavaRDD<String> result = rdd1.subtract(rdd2);
        	System.out.println(result.collect());
        }

- 2.1.5.15 union

        public static void doUnion(JavaSparkContext sc) {
        	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        	JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("d", "e", "f"));
        	JavaRDD<String> result = rdd1.union(rdd2);
        	System.out.println(result.collect());
        }

- 2.1.5.16 intersection

        public static void doIntersection(JavaSparkContext sc) {
        	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "a", "b", "c"));
        	JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "a", "c", "c"));
        	JavaRDD<String> result = rdd1.intersection(rdd2);
        	System.out.println(result.collect());
        }

- 2.1.5.17 join

        public static void doJoin(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("c", 1),
        			new Tuple2("d", 1), new Tuple2("e", 1));
        	List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2("b", 2), new Tuple2("c", 2));
        	JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
        	JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
        	JavaPairRDD<String, Tuple2<Integer, Integer>> result = rdd1.<Integer>join(rdd2);
        	System.out.println(result.collect());
        }

- 2.1.5.18 leftOuterJoin, rightOuterJoin

        public static void doLeftOuterJoin(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", "1"),
        			new Tuple2("c", "1"));
        	List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2("b", 2), new Tuple2("c", "2"));
        	JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
        	JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
        	JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> result1 = rdd1.<Integer>leftOuterJoin(rdd2);
        	JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> result2 = rdd1.<Integer>rightOuterJoin(rdd2);
        	System.out.println("Left: " + result1.collect());
        	System.out.println("Right: " + result2.collect());
        }

- 2.1.5.19 subtractByKey

        public static void doSubtractByKey(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1));
        	List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2("b", 2));
        	JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
        	JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
        	JavaPairRDD<String, Integer> result = rdd1.subtractByKey(rdd2);
        	System.out.println(result.collect());
        }

### **집계 관련 연산**

- 2.1.5.20 reduceByKey

        public static void doReduceByKey(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("b", 1));
        	JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);
        	// Java7
        	JavaPairRDD<String, Integer> result = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
        		@Override
        		public Integer call(Integer v1, Integer v2) throws Exception {
        			return v1 + v2;
        		}
        	});
        	// Java8 Lambda
        	JavaPairRDD<String, Integer> result2 = rdd.reduceByKey((Integer v1, Integer v2) -> v1 + v2);
        	System.out.println(result.collect());
        }

- 2.1.5.21 foldByKey

        public static void doFoldByKey(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("b", 1));
        	JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);
        	// Java7
        	JavaPairRDD<String, Integer> result = rdd.foldByKey(0, new Function2<Integer, Integer, Integer>() {
        		@Override
        		public Integer call(Integer v1, Integer v2) throws Exception {
        			return v1 + v2;
        		}
        	});
        	// Java8 Lambda
        	JavaPairRDD<String, Integer> result2 = rdd.foldByKey(0, (Integer v1, Integer v2) -> v1 + v2);
        	System.out.println(result.collect());
        }

- 2.1.5.22 combineByKey

        public static void doCombineByKey(JavaSparkContext sc) {
        	List<Tuple2<String, Long>> data = Arrays.asList(new Tuple2("Math", 100L), new Tuple2("Eng", 80L),
        			new Tuple2("Math", 50L), new Tuple2("Eng", 70L), new Tuple2("Eng", 90L));
        	JavaPairRDD<String, Long> rdd = sc.parallelizePairs(data);
        	// Java7
        	Function<Long, Record> createCombiner = new Function<Long, Record>() {
        		@Override
        		public Record call(Long v) throws Exception {
        			return new Record(v);
        		}
        	};
        	Function2<Record, Long, Record> mergeValue = new Function2<Record, Long, Record>() {
        		@Override
        		public Record call(Record record, Long v) throws Exception {
        			return record.add(v);
        		}
        	};
        	Function2<Record, Record, Record> mergeCombiners = new Function2<Record, Record, Record>() {
        		@Override
        		public Record call(Record r1, Record r2) throws Exception {
        			return r1.add(r2);
        		}
        	};
        	JavaPairRDD<String, Record> result = rdd.combineByKey(createCombiner, mergeValue, mergeCombiners);
        	// Java8
        	JavaPairRDD<String, Record> result2 = rdd.combineByKey((Long v) -> new Record(v),
        			(Record record, Long v) -> record.add(v), (Record r1, Record r2) -> r1.add(r2));
        	System.out.println(result.collect());
        }

- 2.1.5.23 aggregateByKey

        public static void doAggregateByKey(JavaSparkContext sc) {
        	List<Tuple2<String, Long>> data = Arrays.asList(new Tuple2("Math", 100L), new Tuple2("Eng", 80L),
        			new Tuple2("Math", 50L), new Tuple2("Eng", 70L), new Tuple2("Eng", 90L));
        	JavaPairRDD<String, Long> rdd = sc.parallelizePairs(data);
        	// Java7
        	Record zero = new Record(0, 0);
        	Function2<Record, Long, Record> mergeValue = new Function2<Record, Long, Record>() {
        		@Override
        		public Record call(Record record, Long v) throws Exception {
        			return record.add(v);
        		}
        	};
        	Function2<Record, Record, Record> mergeCombiners = new Function2<Record, Record, Record>() {
        		@Override
        		public Record call(Record r1, Record r2) throws Exception {
        			return r1.add(r2);
        		}
        	};
        	JavaPairRDD<String, Record> result = rdd.aggregateByKey(zero, mergeValue, mergeCombiners);
        	// Java8
        	JavaPairRDD<String, Record> result2 = rdd.aggregateByKey(zero, (Record record, Long v) -> record.add(v),
        			(Record r1, Record r2) -> r1.add(r2));
        	System.out.println(result.collect());
        }

### **pipe 및 파티션 관련 연산**

- 2.1.5.24 pipe

        public static void doPipe(JavaSparkContext sc) {
        	JavaRDD<String> rdd = sc.parallelize(Arrays.asList("1,2,3", "4,5,6", "7,8,9"));
        	JavaRDD<String> result = rdd.pipe("cut -f 1,3 -d ,");
        	System.out.println(result.collect());
        }

- 2.1.5.25 coalesce와 repartition

        public static void doCoalesceAndRepartition(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 10);
        	JavaRDD<Integer> rdd2 = rdd1.coalesce(5);
        	JavaRDD<Integer> rdd3 = rdd2.coalesce(10);
        	System.out.println("partition size:" + rdd1.getNumPartitions());
        	System.out.println("partition size:" + rdd2.getNumPartitions());
        	System.out.println("partition size:" + rdd3.getNumPartitions());
        }

- 2.1.5.26 repartitionAndSortWithinPartitions

        public static void doRepartitionAndSortWithinPartitions(JavaSparkContext sc) {
        	List<Integer> data = fillToNRandom(10);
        	JavaPairRDD<Integer, String> rdd1 = sc.parallelize(data).mapToPair((Integer v) -> new Tuple2(v, "-"));
        	JavaPairRDD<Integer, String> rdd2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(3));
        	rdd2.count();
        	rdd2.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, String>>>() {
        		@Override
        		public void call(Iterator<Tuple2<Integer, String>> it) throws Exception {
        			System.out.println("==========");
        			while (it.hasNext()) {
        				System.out.println(it.next());
        			}
        		}
        	});
        }

- 2.1.5.27 partitionBy

        public static void doPartitionBy(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("apple", 1), new Tuple2("mouse", 1),
        			new Tuple2("monitor", 1));
        	JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data, 5);
        	JavaPairRDD<String, Integer> rdd2 = rdd1.partitionBy(new HashPartitioner(3));
        	System.out.println("rdd1:" + rdd1.getNumPartitions() + ", rdd2:" + rdd2.getNumPartitions());
        }

### **필터와 정렬 연산**

- 2.1.5.28 filter

        public static void doFilter(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        	JavaRDD<Integer> result = rdd.filter(new Function<Integer, Boolean>() {
        		@Override
        		public Boolean call(Integer v1) throws Exception {
        			return v1 > 2;
        		}
        	});
        	System.out.println(result.collect());
        }

- 2.1.5.29 sortByKey

        public static void doSortByKey(JavaSparkContext sc) {
        	List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("q", 1), new Tuple2("z", 1), new Tuple2("a", 1));
        	JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);
        	JavaPairRDD<String, Integer> result = rdd.sortByKey();
        	System.out.println(result.collect());
        }

- 2.1.5.30 keys, values

        public static void doKeysAndValues(JavaSparkContext sc) {
        	List<Tuple2<String, String>> data = Arrays.asList(new Tuple2("k1", "v1"), new Tuple2("k2", "v2"),
        			new Tuple2("k3", "v3"));
        	JavaPairRDD<String, String> rdd = sc.parallelizePairs(data);
        	System.out.println(rdd.keys().collect());
        	System.out.println(rdd.values().collect());
        }

- 2.1.5.31 sample

        public static void doSample(JavaSparkContext sc) {
        	List<Integer> data = fillToN(100);
        	JavaRDD<Integer> rdd = sc.parallelize(data);
        	JavaRDD<Integer> result1 = rdd.sample(false, 0.5);
        	JavaRDD<Integer> result2 = rdd.sample(true, 1.5);
        	System.out.println(result1.take(5));
        	System.out.println(result2.take(5));
        }

## 2.1.6 RDD 액션

---

### **기본 연산**

- 2.1.4.1 collect

        public static void doCollect(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        	List<Integer> result = rdd.collect();
        	for (Integer i : result)
        		System.out.println(i);
        }

- 2.1.4.2 count

        public static void doCount(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        	long result = rdd.count();
        	System.out.println(result);
        }

### **출력 관련 연산**

- 2.1.6.1 first

        public static void doFirst(JavaSparkContext sc) {
        	List<Integer> data = Arrays.asList(5, 4, 1);
        	JavaRDD<Integer> rdd = sc.parallelize(data);
        	int result = rdd.first();
        	System.out.println(result);
        }

- 2.1.6.2 take

        public static void doTake(JavaSparkContext sc) {
        	List<Integer> data = fillToN(100);
        	JavaRDD<Integer> rdd = sc.parallelize(data);
        	List<Integer> result = rdd.take(5);
        	System.out.println(result);
        }

- 2.1.6.3 takeSample

        public static void doTakeSample(JavaSparkContext sc) {
        	List<Integer> data = fillToN(100);
        	JavaRDD<Integer> rdd = sc.parallelize(data);
        	List<Integer> result = rdd.takeSample(false, 20);
        	System.out.println(result.size());
        }

- 2.1.6.4 collect, count
    - 기본 연산 참조
- 2.1.6.5 countByValue

        public static void doCountByValue(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 3));
        	Map<Integer, Long> result = rdd.countByValue();
        	System.out.println(result);
        }

- 2.1.6.6 reduce

        public static void doReduce(JavaSparkContext sc) {
        	List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        	JavaRDD<Integer> rdd = sc.parallelize(data, 3);
        	// Java7
        	int result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
        		@Override
        		public Integer call(Integer v1, Integer v2) throws Exception {
        			return v1 + v2;
        		}
        	});
        	// Java8
        	int result2 = rdd.reduce((Integer v1, Integer v2) -> v1 + v2);
        	System.out.println(result);
        }

- 2.1.6.7 fold

        public static void doFold(JavaSparkContext sc) {
        	List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        	JavaRDD<Integer> rdd = sc.parallelize(data, 3);
        	// Java7
        	int result = rdd.fold(0, new Function2<Integer, Integer, Integer>() {
        		@Override
        		public Integer call(Integer v1, Integer v2) throws Exception {
        			return v1 + v2;
        		}
        	});
        	// Java8
        	int result2 = rdd.fold(0, (Integer v1, Integer v2) -> v1 + v2);
        	System.out.println(result);
        }

- 2.1.6.8 aggregate

        public static void doAggregate(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(100, 80, 75, 90, 95), 3);
        	Record zeroValue = new Record(0, 0);
        	// Java7
        	Function2<Record, Integer, Record> seqOp = new Function2<Record, Integer, Record>() {
        		@Override
        		public Record call(Record r, Integer v) throws Exception {
        			return r.add(v);
        		}
        	};
        	Function2<Record, Record, Record> combOp = new Function2<Record, Record, Record>() {
        		@Override
        		public Record call(Record r1, Record r2) throws Exception {
        			return r1.add(r2);
        		}
        	};
        	Record result = rdd.aggregate(zeroValue, seqOp, combOp);
        	// Java8
        	Function2<Record, Integer, Record> seqOp2 = (Record r, Integer v) -> r.add(v);
        	Function2<Record, Record, Record> combOp2 = (Record r1, Record r2) -> r1.add(r2);
        	Record result2 = rdd.aggregate(zeroValue, seqOp2, combOp2);
        	System.out.println(result);
        }

- 2.1.6.9 sum

        public static void doSum(JavaSparkContext sc) {
        	List<Double> data = Arrays.asList(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d);
        	JavaDoubleRDD rdd = sc.parallelizeDoubles(data);
        	double result = rdd.sum();
        	System.out.println(result);
        }

- 2.1.6.10 foreach, foreachPartition

        public static void doForeach(JavaSparkContext sc) {
        	List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        	JavaRDD<Integer> rdd = sc.parallelize(data);
        	// Java7
        	rdd.foreach(new VoidFunction<Integer>() {
        		@Override
        		public void call(Integer t) throws Exception {
        			System.out.println("Value Side Effect: " + t);
        		}
        	});
        	// Java8
        	rdd.foreach((Integer t) -> System.out.println("Value Side Effect: " + t));
        }

        public static void doForeachPartition(JavaSparkContext sc) {
        	List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        	JavaRDD<Integer> rdd = sc.parallelize(data, 3);
        	// Java7
        	rdd.foreachPartition(new VoidFunction<Iterator<Integer>>() {
        		@Override
        		public void call(Iterator<Integer> it) throws Exception {
        			System.out.println("Partition Side Effect!!");
        			while (it.hasNext())
        				System.out.println("Value Side Effect: " + it.next());
        		}
        	});
        	// Java8
        	rdd.foreachPartition((Iterator<Integer> it) -> {
        		System.out.println("Partition Side Effect!!");
        		it.forEachRemaining(v -> System.out.println("Value Side Effect:" + v));
        	});
        }

- 2.1.6.11 toDebugString

        public static void doDebugString(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd1 = sc.parallelize(fillToN(100), 10);
        	JavaRDD<Integer> rdd2 = rdd1.map((Integer v1) -> v1 * 2);
        	JavaRDD<Integer> rdd3 = rdd2.map((Integer v1) -> v1 * 2);
        	JavaRDD<Integer> rdd4 = rdd3.coalesce(2);
        	System.out.println(rdd4.toDebugString());
        }

- 2.1.6.12 cache, persist, unpersist

        public static void doCache(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(fillToN(100), 10);
        	rdd.cache();
        	rdd.persist(StorageLevel.MEMORY_ONLY());
        }

- 2.1.6.13 partitions

        public static void doGetPartitions(JavaSparkContext sc) {
        	JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 10);
        	System.out.println(rdd.partitions().size());
        	System.out.println(rdd.getNumPartitions());
        }

## 2.1.7 RDD 데이터 불러오기와 저장하기

---

### 2.1.7.1 텍스트 파일

    public static void saveAndLoadTextFile(JavaSparkContext sc) {
    	JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 3);
    	Class codec = org.apache.hadoop.io.compress.GzipCodec.class;
    	// save
    	rdd.saveAsTextFile("<path_to_save>/sub1");
    	// save(gzip)
    	rdd.saveAsTextFile("<path_to_save>/sub2", codec);
    	// load
    	JavaRDD<String> rdd2 = sc.textFile("<path_to_save>/sub1");
    	System.out.println(rdd2.take(10));
    }

### 2.1.7.2 오브젝트 파일

    public static void saveAndLoadObjectFile(JavaSparkContext sc) {
    	JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 3);
    	// save
    	rdd.saveAsObjectFile("<path_to_save>/sub_path");
    	// load
    	JavaRDD<Integer> rdd2 = sc.objectFile("<path_to_save>/sub_path");
    	System.out.println(rdd2.take(10));
    }

### 2.1.7.3 시퀀스 파일

    public static void saveAndLoadSequenceFile(JavaSparkContext sc) {
    	// 아래 경로는 실제 저장 경로로 변경하여 테스트
    	String path = "data/sample/saveAsSeqFile/java";
    	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "b", "c"));
    	// Writable로 변환 - Java7
    	JavaPairRDD<Text, LongWritable> rdd2 = rdd1.mapToPair(new PairFunction<String, Text, LongWritable>() {
    		@Override
    		public Tuple2<Text, LongWritable> call(String v) throws Exception {
    			return new Tuple2<Text, LongWritable>(new Text(v), new LongWritable(1));
    		}
    	});
    	// Writable로 변환 - Java8
    	JavaPairRDD<Text, LongWritable> rdd2_1 = rdd1
    			.mapToPair((String v) -> new Tuple2<Text, LongWritable>(new Text(v), new LongWritable(1)));
    	// SequenceFile로 저장
    	rdd2.saveAsNewAPIHadoopFile(path, Text.class, LongWritable.class, SequenceFileOutputFormat.class);
    	// SequenceFile로 부터 RDD 생성
    	JavaPairRDD<Text, LongWritable> rdd3 = sc.newAPIHadoopFile(path, SequenceFileInputFormat.class, Text.class,
    			LongWritable.class, new Configuration());
    	// Writable을 String으로 변환 - Java7
    	JavaRDD<String> rdd4 = rdd3.map(new Function<Tuple2<Text, LongWritable>, String>() {
    		@Override
    		public String call(Tuple2<Text, LongWritable> v1) throws Exception {
    			return v1._1().toString() + v1._2;
    		}
    	});
    	// Writable을 String으로 변환 - Java8
    	JavaRDD<String> rdd4_1 = rdd3.map((Tuple2<Text, LongWritable> v1) -> v1._1().toString());
    	// 결과 출력
    	System.out.println(rdd4.collect());
    }

## 2.1.8 클러스터 환경에서의 공유 변수

---

- 브로드캐스트 변수

        public static void testBroadcaset(JavaSparkContext sc) {
        	Broadcast<Set<String>> bu = sc.broadcast(new HashSet<String>(Arrays.asList("u1", "u2")));
        	JavaRDD<String> rdd = sc.parallelize(Arrays.asList("u1", "u3", "u3", "u4", "u5", "u6"), 3);
        	// Java7
        	JavaRDD<String> result = rdd.filter(new Function<String, Boolean>() {
        		@Override
        		public Boolean call(String v1) throws Exception {
        			return bu.value().contains(v1);
        		}
        	});
        	// Java8
        	JavaRDD<String> result2 = rdd.filter((String v1) -> bu.value().contains(v1));
        	System.out.println(result.collect());
        }

- 어큐뮬레이터

        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.api.java.function.VoidFunction;
        import org.apache.spark.util.AccumulatorV2;
        import org.apache.spark.util.CollectionAccumulator;
        import org.apache.spark.util.LongAccumulator;
        
        import java.util.Arrays;
        import java.util.List;
        
        public class AccumulatorSample {
        
          // ex 2-141
          public static void runBuiltInAcc(JavaSparkContext jsc) {
            LongAccumulator acc1 = jsc.sc().longAccumulator("invalidFormat");
            CollectionAccumulator acc2 = jsc.sc().collectionAccumulator("invalidFormat2");
            List<String> data = Arrays.asList("U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7");
            jsc.parallelize(data, 3).foreach(new VoidFunction<String>() {
              @Override
              public void call(String v) throws Exception {
                if (v.split(":").length != 2) {
                  acc1.add(1L);
                  acc2.add(v);
                }
              }
            });
            System.out.println("잘못된 데이터 수:" + acc1.value());
            System.out.println("잘못된 데이터:" + acc2.value());
          }
        
          // ex 2-144
          public static void runCustomAcc(JavaSparkContext jsc) {
            RecordAccumulator acc = new RecordAccumulator();
            jsc.sc().register(acc, "invalidFormat");
            List<String> data = Arrays.asList("U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7");
            jsc.parallelize(data, 3).foreach(new VoidFunction<String>() {
              @Override
              public void call(String v) throws Exception {
                if (v.split(":").length != 2) {
                  acc.add(new Record(1L));
                }
              }
            });
            System.out.println("잘못된 데이터 수:" + acc.value());
          }
        
          public static void main(String[] args) throws Exception {
            SparkConf conf = new SparkConf().setAppName("AccumulatorSample").setMaster("local[*]");
            JavaSparkContext sc = new JavaSparkContext(conf);
            //runBuiltInAcc(sc);
            runCustomAcc(sc);
            sc.stop();
          }
        }
        
        class RecordAccumulator extends AccumulatorV2<Record, Long> {
        
          private static final long serialVersionUID = 1L;
        
          private Record _record = new Record(0L);
        
          @Override
          public boolean isZero() {
            return _record.amount == 0L && _record.number == 1L;
          }
        
          @Override
          public AccumulatorV2<Record, Long> copy() {
            RecordAccumulator newAcc = new RecordAccumulator();
            newAcc._record = new Record(_record.amount, _record.number);
            return newAcc;
          }
        
          @Override
          public void reset() {
            _record.amount = 0L;
            _record.number = 1L;
          }
        
          @Override
          public void add(Record other) {
            _record.add(other);
          }
        
          @Override
          public void merge(AccumulatorV2<Record, Long> otherAcc) {
            try {
              Record other = ((RecordAccumulator) otherAcc)._record;
              _record.add(other);
            } catch (Exception e) {
              throw new RuntimeException();
            }
          }
        
          @Override
          public Long value() {
            return _record.amount;
          }
        }

[wikibook/spark2nd](https://github.com/wikibook/spark2nd/tree/master/src/main/java/com/wikibooks/spark/ch2)