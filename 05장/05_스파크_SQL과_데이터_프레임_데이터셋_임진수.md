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
