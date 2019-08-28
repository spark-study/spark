# 5장 스파크 SQL과 데이터 프레임, 데이터셋

- 스파크 SQL

```
- RDD를 사용은 분산환경에서 메모리 기반으로 빠르고 안정적으로 동작하는 프로그램을 작성할 수 있는 점
- but 데이터에 대한 메타데이터를 표현할 방법이 없어서 불편
- 스파크 SQL는 RDD의 이같은 단점을 보완하고 다른 유형의 데이터 모델과 API를 제공하는 스파크 모듈
```

- 언어별 데이터셋, 프레임 지원
  - scala : 데이터 프레임, 데이터셋 클래스
  - java : 데이터셋 클래스
  - python, R : 데이터 프레임 클래스
- 데이터셋 예제 

```scala
val rdd = sc.parallelize(List(1, 2, 3))
println(rdd.collect.mkString(", "))
// print : 1, 2, 3

val ds = List(1, 2, 3).toDS

ds.show
// 내용물을 MySQL 출력하듯이 나옴
ds.printSchema
// 스키마 정보 출력
```



#### 5.1 데이터셋

- RDD와 똑같이 분산 오브젝트 컬렉션에 대한 프로그래밍 모델
- 트랜스포메이션과 액션 연산을 포함
- 데이터 프레임은 RDD에 비해 풍부한 API와 옵티마이저를 기반으로 한 높은 성능이 있지만 처리해야하는 작업에 따라 코드가 복잡해지는 문제가 있었음
- 데이터 셋은 이러한 데이터프레임이 제공하던 기능을 유지하면 컴파일 타임 오류 체크 등의 기능을 추가



#### 5.2 연산의 종류와 주요 API

- 데이터셋도 RDD랑 똑같이 트랜스포메이션과 액션으로 메소드가 분류됨
  - 기준도 동일
- 트랜스포메이션 연산이 2가지 종류
  1. 타입 연산(typed operations)
  2. 비타입 연산(untyped operations)
     - 데이터를 처리할 때, 본래의 타입이 아닌 org.apache.spark.sql.Row, org.apache.spark.sql.Column 타입의 객체로 감싸서 하는 연산



#### 5.3 코드 작성 절차 및 단어수 세기 예제

1. pom.xml 파일에 스파크SQL 모듈에 대한 의존성 정보를 설정하는 것

   ```xml
   <dependency>
   	<groupId>org.apache.spark</groupId>
   	<artifactId>spark-sql_2.11</artifactId>
   	<version>2.3.0</version>
   </dependency>
   ```

   ```xml
   # 하이브와 연동할 목적
   <dependency>
   	<groupId>org.apache.spark</groupId>
   	<artifactId>spark-hive_2.11</artifactId>
   	<version>2.3.0</version>
   </dependency>
   ```

2. 코드 작성
   1. 스파크세션 생성
   2. 스파크세션으로부터 데이터셋 또는 데이터프레임 생성
   3. 생성된 데이터셋 또는 데이터프레임을 이용해 데이터 처리
   4. 처리된 결과 데이터를 외부 저장소에 저장
   5. 스파크 세션 종료



#### 5.4 스파크세션

- 스파크세션은 데이터 프레임 또는 데이터 셋을 생성하거나 사용자 정의함수를 등록하기 위한 목적으로 사용

  ```scala
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
  			.builder()
  			.appName("sample")
  			.master("local[*]")
  			.getOrCreate()
  ```

  

- 스파크세션은 스파크 2.0 버전부터 사용된 것, 기존에는 SQLContext, HivrContext를 사용했었음
- 스파크 2.0에서는 두 클래스를 합친 스파크 세션 클래스를 정의, 하나로 하이브 지원까지 가능

#### 5.5 데이터프레임, 로우, 컬럼

- 데이터프레임 : 스파크SQL에서 사용하는 분산 데이터 모델
  - RDD랑 달리 데이터 값 + 스키마 정보
  - RDD를 통해서 얻기 어려운다양한 성능 최적화 기능을 제공

###### 5.5.1 데이터 프레임 생성

- 스파크세션을 이용하여 생성



###### 5.5.1.1 외부 데이터 소스로부터 데이터프레임 생성

- 파일이나 데이터베이스 같은 외부 저장소에 저장돼 있는 데이터를 이용하여 데이터 프레임을 생성
- 스파크 세션이 제공하는 read() 메서드를 이용

```scala
val df = spark.read.format("json").
			option("allowComments", "true").
			load("<file_path>")
```

- format() : "orc", "kafka", "csv", ... 등등 읽어들이고자 하는 데이터소스의 유형을 문자열로 지정
- option/options() : 데이터 소스에 사용할 설정 정보를 지정할 수 있습니다.
- load(), jdbc(), json() ... 각 형식에 맞게 데이터 프레임 생성

###### 5.5.1.2 기존 RDD 및 로컬 컬렉션으로부터 데이터프레임 생성

- 이미 생성된 다른 RDD 또는 데이터 프레임을 이용하여 생성 할 때, 데이터에 관한 스키마 정보를 함께 지정해야함



###### 5.5.1.2.1 리플렉션을 통한 데이터프레임 생성

- 로우와 칼럼 형태로 만들수 있는 컬렉션 객체를 생성하고 새로운 데이터 프레임을 생성

```python
row1 = Row(name="hayoon", age=7, job="student")
row2 = Row(name="sunwoo", age=13, job="student")
row3 = Row(name="hajoo", age=5, job="kindergartener")
row4 = Row(name="jinwoo", age=13, job="student")
data = [row1, row2, row3, row4]
df = spark.createDateFrame(data)
```



###### 5.5.1.2.2 명시적 타입 지정을 통한 데이터프레임 생성

- 리플렉션 방식을 통한 데이터프레임 생성 방법은 스키마 정보를 일일히 지정하지 않아도 되지만, 데이터 프레임 생성을 위하여 케이스 클래스 같은 것들을 따로 정의해야하는 불편함이 존재

```python
sf1 = StructField("name", StringType(), True)
sf2 = StructField("age", IntegerType(), True)
sf3 = StructField("job", StringType(), True)
schema = StructType([sf1, sf2, sf3])
r1 = ("hayoon", 7, "student")
r2 = ("sunwoo", 13, "student")
r3 = ("hajoo", 5, "kindergartener")
r4 = ("jinwoo", 13, "student")
rows = [r1, r2, r3, r4]
df6 = spark.createDataFrame(rows, schema)
```

- 스파크 SQL에서 지원하는 데이터 타입은 다양, Byte, Short ..., String, Binary, Timestamp, Date, Array ...



###### 5.5.1.2.3 이미지 파일을 이용한 데이터프레임 생성

- 스파크 2.3.0 부터는 이미지 파일을 이용해여 데이터 프레임 생성이 가능
- 이미지파일이 있는 경로를 지정하여 파일을 생성하면 됨

```scala
val df = ImageSchema.readImages(path, spark, recursive, numParitions, dropImageFailures, sampleRatio, seed)
```

- 실행시 이미지의 전체 경로와 가로, 세로, 채널 정보가 포함되어서 출력



##### 5.5.2 주요 연산 및 사용법

###### 5.5.2.1 액션 연산

- RDD랑 동일, 액션 연산이 호출될 때만 실제 연산이 수행

```python
df.show()
df.head()
df.first()
df.take(2)
df.count()
df.collect()
```



###### 5.5.2.2 기본 연산

- 데이터셋이 제공하는 연산은 크게 **기본 연산**, **타입 트랜스포메이션 연산**, **비타입 트랜스포메이션 연산**, **액션 연산**으로 나뉨

```python
df.persist(StorageLevel.MEMORY_AND_DISK_2)
df.printSchema()
df.columns
df.dtypes
df.schema
```



###### 5.5.2.3 비타입 트랜스포메이션 연산

- 데이터의 실제 타입을사용하지 않는 변환 연산을 수행한다는 의미



###### 5.5.2.4 Row, Column, Functions

- 비타입 트랜스포메이션 연산을 사용할 때는 Row와 Column, functions 이라는 세 가지 이해 필요
- 10살 초과하는 사람을 찾는 코드(where은 SQL에서 "where"랑 대응)

```python
df.where(df.age > 10).show()
```

- 기타 SQL 문에서 각 기능에 해당하는 함수들이 잔득

  -  alias() : 

  - isin(), : in

  - when() : SQL에서 case 랑 동일

  - count, sum, min, mean ...

  - grouping : 기준으로 group

    ```python
    df2.cube(df2["store"], df2["product"]).agg(sum(df2["amount"]), grouping(df2["store"])).show(truncate=False)
    ```

    

- pandas 연동
  - 스파크 2.3.0 버전 부터 아파치 Arrow 플랫폼을도입해 스파크 데이터프레임과 파이썬의 pandas, numpy 라이브러리 간의 빠른 데이터 교환이 가능



#### 5.6 데이터셋

- 데이터 프레임은 데이터셋 모델의 일부로 통합되어 있음
  - 데이터 프레임은 Dataset[Row] 타입의 데이터셋을 가리키는 별칭
  - 데이터셋이 생긴 이유 → 데이터 프레임과 RDD 간의 데이터 타입을 다루는 방식의 차이 때문

```scala
// 데이터 프레임에서는 각 문자별로 count 문제 없음
val df = sc.parallelize(List("a", "b", "a")).toDF("word")
df = df.withColumn("count", lit("1")).groupBy("word").agg(sum("count"))
```

```scala
val rdd = sc.parallelize(List("a", "b", "a"))
rdd = rdd.map((_, 1L)).reduceByKey(_ +_)
// error 발생되는 코드, RDD 에서는 재할당 하는 객체의 타입이 달라서 에러발생
```

- rdd 변수의 타입은 RDD[String] 인데 재할당하려는 값은 RDD[String, Long] 타입이기 때문
- 데이터프레임은 내부의 데이터가 Row의 집합이라는 것만 보장돼 있을 뿐, 실제 데이터 타입에 대한 정보 X
  - 그래서, RDD 에서는 Map 내부에서 내부 데이터가 String 이라면 String의 내장함수를 사용할 수 있지만 데이터셋은 불가능

- 데이터셋은 JVM 기반 언어인 자바와 스칼라만 사용가능



###### 5.6.1 데이터셋 생성

- 기존과 비슷

###### 5.6.1.1 파일로부터 생성

```scala
val df1 = spark.read.textFile("<FilePath>")
```

- read() 메소드를 이용해서 데이터프레임을 위한 DataFrameReader 객체를 생성하고 textFile() 메서드를 이용해 Dataset[String] 타입의 데이터셋을 생성



###### 5.6.1.2 자바 객체를 이용해 생성

- 리플렉션 방식의데이터 프레임 생성과 유사
  - scala의 경우 scala.Product 하위의 객체로 구성된 컬렉션을 이용
  - java는 자바빈 컬렉션을 이용해 생성

```scala
// 기존 객체를 이용한 데이터셋 생성
case class Person(name: String, age:Int, job: String)
// 스칼라, 기본 타입에 대한 인코더 정보를 암묵적 변환 방식을 적용
import spark.implicits._
val row1 = Person("hayoon", 7, "student")
val row2 = Person("sunwoo", 13, "student")
val row3 = Person("hajoo", 5, "kindergartener")
val row4 = Person("jinwoo", 13, "student")
val data = List(row1, row2, row3, row4)
val df2 = spark.createDataset(data)
val df2_1 =data.toDS()
```

- 인코더 : 자바 객체와 스파크 내부 바이너리 포맷 간의 변환을 처리하기 위한 것
  - org.apache.spark.sql.Encoder
  - 스칼라 사용시 기본은 위처럼 import를 하면되지만, 기본 타입이 아니거나 자바를 사용할 시 인코더 생성 메서드를 이용해 인코더를 생성 및 지정해야함

```scala
val e1 = Encoders.BOOLEAN
val e2 = Encoders.LONG
val e3 = Encoders.scalaBoolean
val e5 = Encoders.scalaLong
val e6 = Encoders.javaSerialization[Person]
val e7 = Encoders.kryo[Person]
val e8 = Encoders.tuple(Encoders.STRING, Encoders.INT)
```

- Encoders는 인코더 생성을 위한 일정의 팩토리 클래스처럼 사용할 수 있음



###### 5.6.1.3 RDD 및 데이터 프레임을 이용해 생성

- 문제 해결에 더욱 편리한 API를 사용할 목적으로 데이터프레임과 데이터셋, RDD 간의 변환을 수행해야하는 경우 자주 사용

```scala
// RDD로부터 생성
import spark.implicits._
val rdd = sc.parallelize(List(1, 2, 3))
val ds3 = spark.createDataset(rdd)
val ds4 = rdd.toDS()
```

```scala
// 데이터프레임으로부터 생성
val ds = List(1, 2, 3).toDF.as[Int]
```



###### 5.6.1.4 Range()로 생성

```scala
val ds = spark.range(0, 10, 3)
```



###### 5.6.2 타입 트랜스포메이션 연산

- 데이터셋이 제공하는 연산은 기본연산, 타입/비타입 트랜스포메이션연산, 액션 연산

###### 5.6.2.1~5.6.2.9 다양한 타입 트랜스메이션 연산들

- select, as distinct, dropDuplicates,filter, map, flatMap, groupByKey, agg, mapValues, reduceGroups
  - map, flatmap → 데이터 프레임에서는 데이터의 타입과 무관하기 때문에 Row 타입의 객체로 변환해서 다뤄야 했지만, 데이터셋에서는 원래의 데이터 타입을 그대로 사용할 수 있음



###### 5.7 하이브 연동

- 하이브는 SQL 기반의 데이터 처리 기능을 제공하는 오픈소스 라이브러리로, 하이브QL을 사용하고 있는 데이터웨어하우스 시스템
- 스파크에서는 표준 SQL 외에 하이브QL을 사용할 수 있도록 지원

```scala
spark.sql("CREATE TABLE Persons (name STRING, age INT, job STRING)")
spark.sql("show tables").show
```

- 스파크에서 설정을 통해서 외부 하이브랑 연동도 가능 → 연동 후 스파크에서 생성한 테이블을 하이브에서도 볼 수 있음

```scala
// Users라는 이름의 테이블로 저장
import org.apache.spark.sql.SaveMode
ds.toDF().write.format("hive").mode(SaveMode.Overwrite).saveAsTable("Users")
```



###### 5.8 분산 SQL 엔진

- 스파크 SQL은 그 자체가 분산 SQL 서버로 동작해서 다른 프로그램이 JDBC 또는 ODBC 방식으로 접속한 뒤 스파크에서 생성한 테이블을 사용할 수 있는 기능을 제공

  - bin/start-thriftserver.sh 실행만 하면됨

  

###### 5.9 Spark SQL CLI

- 스파크가 제공하는 또 다른 명령행 툴
  - 명령 프롬프트에서 하이브에서 사용하던 방법으로 쿼리를 실행할 수 있음



###### 5.10 퀴리플랜(Query Plan)과 디버깅

- RDD가 있어도 데이터프레임/데이터셋을 사용하는 이유는 최적화를 통한 성능 개선 가능성때문



###### 5.10.1 스파크세션과 세션스테이트, 스파크컨텍스트

- RDD를 생성하려면 스파크컨텍스트 객체를 먼저 생성하고, 데이터 프레임 또는 데이터셋을 생성 하려면 스파크세션 객체를 먼저 생성해야함
  - but, 스파크세션 객체 안에는 스파크컨텍스트 객체가 포함돼 잇음
  - so, RDD or 데이터프레임을 만들때나 스파크 세션 객체를 생성하면 됨
- SparkSession 내부적으로 sparkContext를 가지고 있음
  - sparkContext : 벡엔드 서비스들에 직접 접근할 수 있는 참조 변수들을 가지고 있음, 이를 통해서 RDD API에서 요청한 작업을 수행하게됨
  - 하지만 사용자가 작성한 코드와 실제 벡엔드 서비스 API사이에 직접적인 연관 관계가 형성될 수 있어서 프레임워크 차원에서 뭔가 최적화된 기능을 수행하기 어려움
  - 그래서 데이터 프레임과 데이터셋을 추상화하여 내부적인 최적화를 거쳐서 RDD 기반의 코드를 작성하게 됨
- SparkState : 세션 상태 정보를 저장하고 관리하는데 사용되는 클래스
  - 크게 핵심은 analzer, optimizer, SparkPlanner, QueryExecution



###### 5.10.2 QueryExecution

- 데이터 프레임을사용할 경우 스파크가 내부적으로 최적화 과정을 거쳐 실제 실행 코드를 생성
- queryExcution은 변환 과정에서 일어나는 일을 사용자가 확인할수 있게 특화된 API
  - logical : 논리적으로 분석
  - analyzed : 분석하여 식별등
  - optimizedPlan : 최적의 플랜을 찾음
  - sparkPlan : 구체적 처리 방법에 대한 정보
  - exutedPlan : sparkPlan 실행 계획에 코드 제너레이션과 같은 기법을 추가해서 셔플 및 내부 바이너리 코드 최적화까지 고려한 최종 쿼리 실행 계획



###### 5.10.3 LogicalPlan과 SparkPlan

- 스파크는 최적화 처리 시 논리적 실행 계획을 나타내느 LogicalPlan과 구체적인 실행 계획을 나타내는 SparkPlan이라는 두 개의 클래스를 사용
- TreeNode -> QueryPlan을 둘다 상속 받음 : 일종의 트리 구조
  - 사용자가 작성한 코드를 구문 트리로 변환해서 최적화를 수행하게 됨
