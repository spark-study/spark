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



5.5.2.3 비타입 트랜스포메이션 연산

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

