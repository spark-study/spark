---
layout: post
title:  "[스파크2 프로그래밍] 5장_스파크SQL 과 데이터프레임,데이터셋"
date:   2019-08-21
categories: Spark
---

RDD 의 장점은, 

- 분산환경에서 메모리 기반으로 빠르고 안정적으로 동작하는 프로그램을 만들 수 있다는 것과, 
- RDD 가 제공하는 풍부한 데이터 처리 연산입니다. ( <-> 맵과 리듀스로만 문제를 해결 ) 

RDD 의 단점은,

- 스키마를 표현할 수 없다는 것입니다. 

스파크 SQL 은 RDD 의 이 단점을 보완할 수 있도록 또 다른 유형의 데이터 모델과 API를 제공하는 스파크 모듈입니다. 스파크 SQL 에서 데이터를 다루는 방법은 SQL을 사용하는 것과 데이터셋 API 를 사용하는 방법이 있습니다. 

데이터셋은 스파크 1.6 버젼에서 처음 소개된 것으로 자바와 스칼라 언어에서만 사용할 수 있었고, 그 이전에는 데이터프레임이라는 클래스를 구현 언어와 상관없이 사용하고 있었습니다. **스파크 2.0 부터 데이터프레임 클래스가 데이터셋 클래스로 통합**되면서 Type Alias 라는 독특한 기능을 가진 스칼라 언어에서만 기존과 같은 데이터 프레임을 사용할 수 있고 해당 기능이 없는 자바에서는 데이터셋 클래스만을 사용할 수 있게 됐습니다. 

스칼라의 데이터프레임은 "type DataFrame = Dataset[Row]" 와 같이 정의돼 있는데 바로 이 구문이 Type Alias 에 해당하는 부분으로, **Dataset 의 타입 파라미터가 Row 인 경우를 DataFrame** 이라는 이름으로도 사용하겠다는 의미입니다.

```scala
val ds = List(1, 2, 3).toDS
ds.show
ds.printScheme
```

### 5.1 데이터셋

데이터셋 이전에는 데이터프레임이라는 API 를 사용했습니다. 가장 큰 특징은, 기존 RDD 와는 다른 형태를 가진 **SQL 과 유사한 방식의 연산을 제공**했다는 점입니다. 예를 들어,

```scala
rdd.map(v=>v+1)
```

와 같은 map() 연산을 사용한 것에 반해, 동일한 요소로 구성된 데이터프레임에서는 

```scala
df.select(df("value")+1)
```

와 같은 방법을 사용했습니다. df는 데이터프레임을 df("value") 는 데이터프레임이 가지고 있는 "value" 라는 이름의 칼럼을 의미하는 것으로, 마치 SQL 의 SELECT 문과 유사한 형식으로 데이터를 조회하는 방법입니다.

이러한 데이터 프레임의 장점은, 

- 풍부한 API 와 옵티마이저를 기반으로 한 높은 성능으로 복잡한 데이터 처리를 수월하게 수행할 수 있습니다.

하지만,

- 처리해야하는 작업의 특성에 따라 RDD 를 사용하는것에 비해 복잡한 코드를 작성하거나 컴파일 타임 오류 체크 기능을 사용할 수 없는 단점이 있습니다.

데이터셋은 데이터프레임이 제공하던 **성능 최적화 같은 장점을 유지하면서 RDD 에서만 가능했던 컴파일 타임 오류 체크 등의 기능을 사용**할 수 있게 됐습니다. 

### 5.2 연산의 종류와 주요 API

데이터셋이 제공하는 연산은 RDD 와 마찬가지로 두 종류로 분류합니다.

- 트렌스포메이션 
  - 새로운 데이터셋을 생성하는 연산. (액션 연산이 호출될 때까지 수행되지 않습니다)
- 액션연산
  - 실제 데이터 처리를 수행하고 결과를 생성하는 연산 

트랜스포메이션 연산은 데이터 타입을 처리하는 방법에 따라 두 가지로 나뉩니다.

- 타입 연산
- 비타입 연산

```scala
scala> val data = 1 to 100 toList
scala> val ds = data.toDS //데이터셋 생성
scala> val result = ds.map(_+1) // == ds.select(col("value") + 1)
```

```scala
ds.select(col("value") + 1)
```

이것은 데이터셋을 데이터베이스의 테이블과 유사하게 처리한 것으로, 마치 value 라는 칼럼을 가진 ds 라는 테이블에서 value 칼럼에 +1 을 한 결과를 조회하는 것과 유사한 방법입니다. 이 때, col("value") 라는 부분이 칼럼을 나타내며, 이 타입이 원래 데이터 타입인 정수가 아니라 org.apache.spark.sql.Column 타입입니다.

데이터셋에는 SQL 과 유사한 방식의 데이터 처리를 위해 데이터베이스의 Row 와 Column 에 해당하는 타입을 정의하고 있으며, 실제 데이터가 어떤 타입이든지 로우와 칼럼 타입으로 감싸러 처리할 수 있는 구조입니다.

**비타입 연산이란, 데이터를 처리할 때 데이터 본래의 타입이 아닌 org.apache.spark.sql.Column 과 org.apache.spark.sql.Row  타입의 객체로 감싸서 처리하는 연산**입니다.

데이터프레임은 org.apache.spark.sql.Row  타입의 요소로 구성된 데이터셋을 가리키는 용어입니다. 모든 데이터를 Row 타입으로 변환해서 생성된 데이터셋을 가리켜 데이터프레임이라는 별칭으로 부르기 대문에 이런 연산들을 데이터 프레임 연산이라고 부르기도 합니다.

### 5.3 코드 작성 절차 및 단어 수 세게 예제

단계별 스파크 SQL 코드 작성 방법입니다.

1. 스파크 세션 생성 (RDD 를 생성하기 위해 SparkContext가 필요한 것처럼, 데이터 프레임을 생성하기 위해 필요)

   ```java
   SparkSession spark = SparkSession
     .builder()
     .appName("jko") //application name
     .master("local[*]") //master info
     .getOrCreate();
   ```

2. 스파크세션으로부터 데이터셋 or 데이터프레임 생성

   ```java
   String source = "file://<spark_home_dir>/README.md";
   Dataset<Row> df = spark.read().test(source); //read() 는 DataFrameReader 인스턴스 리턴
   ```

3. 생성된 데이터셋 또는 데이터프레임을 이용해 데이터 처리

   ```scala
   val ds = df.as[(String)] // 데이터 프레임을 데이터 셋으로 변환. Row 타입 요소 -> 원래 데이터 타입
   val wordDF = ds.flatMap(_.split(" ")) // 문장을 각 단어로 분리
   val result = wordDF.groupByKey(v => v).count // 같은 단어끼리 그룹 생성하고 그룹별 요소 개수 count
   ```

4. 처리된 결과 데이터를 외부 저장소에 저장

   ```scala
   result.write.test("<저장경로>") // write() 는 DataFrameWriter 를 리턴
   ```

5. 스파크 세션 종료

### 5.4 스파크세션

데이터프레임 또는 데이터셋을 생성하거나 사용자 정의 함수를 등록하기 위한 목적으로 사용됩니다. 스파크 SQL 2.0 부터 사용된 것으로, 기존에는 SQLContext 와 HiveContext를 사용했습니다.

기존 SQLContext 는 스파크세션과 유사하게 데이터 프레임을 생성하고 사용자 정의 함수를 등록하는 기능을 수행했는데, 아파치  하이브에서 제공하는 **HiveQL 을 사용하거나 기존 하이브 서버와 연동하기 위해서는 SQLContext 의 하위 클래스인 HiveContext 를 사용해야**했습니다.

이 두 클래스를 합친 **스파크세션 클래스를 정의하면서 스파크 세션 하나만으로 하이브 지원까지 가능**합니다. 

하이브 서버와 연동해서 사용하고 싶으면, 스파크의 conf 디렉토리에 hive-site.xml, core-site.xml., hfs-site.xml 파일을 생성해야 합니다. 하이브 및 하둡에서 사용하는 설정파일입니다.

### 5.5 데이터프레임, 로우, 칼럼

**데이터프레임은 org.apache.spark.sql.Row 타입의 요소를 가진 데이터셋**을 가리키는 별칭으로 사용되고 이습니다. R 의 데이터프레임이나 데이터베이스의 테이블과 비슷한 행과 열의 구조를 가지고 있어서, 데이터프레임에 포함된 데이터는 SQL 문을 사용하거나 데이터프레임이 제공하는 프로그래밍 API 를 이용해 처리 가능합니다.

데이터프레임을 사용하는 궁긍적인 목적은 RDD 와 같이 분산 데이터를 저장하고 처리하기 위한것이지만 RDD 가 데이터의 값을 다루는데 초점을 맞추고 있었다면 **데이터프레임은 데이터 값뿐만 아니라 제공된 스키마 정보를 이용해 RDD 를 통해서는 얻기 힘든 다양한 성능 최적화 기능을 제공**하기 때문입니다.

#### 5.5.1 데이터프레임 생성

스파크 세션을 이용해 생성합니다. 

생성 방법은 **파일이나 데이터베이스와 같은 스파크 외부의 데이터소스에 저장된 데이터를 이용해 생성할 수도 있고 이미 생성돼 있는 다른 RDD 나 데이터프레임에 변환 연산을 적용해 새로운 데이터프레임을 생성**할 수도 있습니다.

다음은 생성 방법 입니다.

- 외부데이터로부터
- 기존 RDD 및 로컬 컬렉션으로부터

##### 5.5.1.1 외부 데이터소스로부터 데이터프레임 생성

파일이나 데이터베이스 같은 외부저장소에서 데이터를 읽어서 데이터프레임을 생성할 때 가장 손쉽게 처리할 수 있는 방법은 스파크 세션이 제공하는 read() 메서드를 사용하는 것입니다. read() 메서드는 DateFrameReader 인스턴스를 생성하는데, 이를 이용해 다양한 유형의 데이터를 일고 데이터프레임을 생성할 수 있습니다.

##### 5.5.1.2 기존 RDD 및 로컬 컬력센으로부터 데이터 프레임 생성

스파크 SQL 은 스키마를 지정하는 두 가지 방법을 제공합니다. 

- 리플렉션 API 를 이용해 데이터의 스키마 정보를 자동으로 추론하는 방법
  - 스키마의 정의 위한 별도의 추가 코드가 필요 없어서 간결한 코드를 작성할 수 있다는 장점
- 개발자가 직접 스키마 정보를 코드로 작성해서 지정
  - 스키마 추론을 위한 부가적인 연산을 줄이고 스키마 정보를 원하는 대로 커스터마이징해서 사용하고자 한다면 이 방법을 사용

###### 5.5.1.2.1 리플렉션을 통한 데이터프레임 생성

**데이터 프레임 내부의 데이터는 동일한 수의 칼럼 정보를 포함하고 있는 로우의 집합**입니다. 그래서 RDD 를 비롯해 로우와 칼럼 형태로 만들 수 있는 컬렉현 객체만 있다면 이를 이용해 새로운 데이터 프레임을 만들 수 있습니다.

다음은 RDD 가 아닌 일반 리스트를 이용해 데이터 프레임을 생성하는 예제입니다.

```java
public static calss Person implements Serializable{
  private String name;
  private int age;
  private String job;
  ...
}

Person row1 = new Person("junhee", 6, "student");
Person row2 = new Person("bill", 12, "god");
Person row3 = new Person("jesi", 14, "mom");
Person row4 = new Person("mina", 41, "chef");

List<Person> data = Arrays.asList(row1, row2, row3, row4);		
Dataset<Row> df4 = spark.createDataFrame(data, Person.class);

df4.show();
```

**특별한 스키마 정의를 추가히지 않아도 컬렉션에 포함된 오브젝트의 속성값으로부터 알아서 스키마 정보를 추출하고 데이터 프레임을 만드는 방법을 리플렉션을 이용한 데이터 프레임 생성 방법**이라고 합니다. 

다음은 RDD 로부터 데이터프레임을 생성하는 예제입니다.

```java
public static calss Person implements Serializable{
  private String name;
  private int age;
  private String job;
  ...
}

Person row1 = new Person("junhee", 6, "student");
Person row2 = new Person("bill", 12, "god");
Person row3 = new Person("jesi", 14, "mom");
Person row4 = new Person("mina", 41, "chef");

List<Person> data = Arrays.asList(row1, row2, row3, row4);	

JavaRDD<Person> rdd = sc.parallelize(data);
Dataset<Row> df5 = spark.createDataFrame(rdd, Person.class);
```

###### 5.5.1.2.2 명시적 타입 지정을 통한 데이터 프레임 생성

리플렉션 방식은 스키마를 정의하지 않다는 점이 편리하지만, 데이터프레임 생성을 위한 케이스 클래스를 따로 정의해야하는 불편함이 있고 상황에 따라 원하는 대로 직접 스키마 정보를 구성할 수 있는 방법이 편리할 수 있습니다.

스파크 SQL 은 개발자들이 직접 스키마를 지정할 수 있는 방법을 제공하는데 리플렉션 방식을 설명할때 사용한 것과 동일한 데이터 프레임을 생성하는 예제를 통해 그 차이점을 보겠습니다.

```java
StructField sf1 = DataTypes.createStructField("name", DataTypes.StringType, true);
StructField sf2 = DataTypes.createStructField("age", DataTypes.IntegerType, true);
StructField sf3 = DataTypes.createStructField("job", DataTypes.StringType, true);
StructType scheme = DataTypes.createStructField(Arrays.asList(sf1, sf2, sf3, sf4));

Row r1 = RowFactory.create("junhee", 6 "student");
Row r2 = RowFactory.create("bill", 12 "god");
Row r3 = RowFactory.create("jesi", 14 "mom");
Row r4 = RowFactory.create("mina", 41 "chef");

List<Row> rows = Arrays.asList(r1, r2, r3, r4);
Dataset<Row> df6 = spark.createDataFrame(rows, scheme);
```

데이터 프레임의 스키마 정보는 **칼럼을 나타내는 StructField 와 로우를 나타내는 StructType** 으로 정의하는데, StructField 에는 칼럼의 이름과 타입, null 허용 여부를 지정하고, StructType 에는 칼럼으로 사용할 StructField 목록을 지정합니다.

StructField 와 StructType 정보를 구성햇다면, 데이터 프레임을 생성할 데이터를 준비합니다. 스키마 정보에 맞춰 Row 객체를 생성하는 과정입니다.

모든 준비가 끝나면, 스파크 세션이 제공하는 createDataFrame 메서드를 이용해 데이터프레임을 생성합니다.

###### 5.5.1.2.3 이미지 파일을 이용한 데이터 프레임 생성

#### 5.5.2 주요 연산 및 사용법

**데이터 프레임은 org.apache.spark.sql.Row 타입의 객체로 구성된 데이터셋**을 가리키는 용어입니다. 스칼라 API 에서는 데이터셋의 선언부를 보면 Dataset[T] 라고 되어 있는데, Dataset[Int], Dataset[String] 등은 그냥 데이터셋, 유일하게 Dataset[Row] 경우에만 데이터프레임이라는 용어를 사용합니다.

**이렇게 동일한 타입을 구분해서 사용하는 이유는 Dataset[Row] 인 경우 사용 가능한 트랜스포매이션 연산의 종류가 달라지기 때문**입니다.

지금부터 데이터셋이 제공하는 주요 연산을 살펴보면서 데이터프레임에 특화된 것들은 어떤 것들이 있는지 확인합니다.

데이터셋이 제공하는 연산은 크게

- 액션 연산
- 기본 연산
- 비타입 트랜스포매이션 연산
  - 데이터셋의 구성요소가 org.apache.spark.sql.Row 타입인 경우, 즉 **데이터 프레임인 경우에만** 사용 가능
- 타입 트랜스포매이션 연산
  - **데이터프레임이 아닌 데이터셋**의 경우에만 사용 가능

##### 5.5.2.1 액션 연산

데이터셋은 RDD 와 마찬가지로 액션 연산과 트렌스포매이션 연산을 제공하며, 액션 연산이 호출될 경우 실제 여산인 수행됩니다. 이는 데이터 프레임이 제공하는 비타입 트렌스포메이션 연산의 경우도 마찬가지고, **액션 연산을 호출해야만 트렌스포메이션 연산의 결과를 확인 할 수 있습니다**.

- show()
- head(), first()
- take()
- count()
- collect(), collectAsList()
- describe()

##### 5.5.2.2 기본 연산 

다음은 데이터 셋이 제공하는 기본 연산입니다.

- cache(), persist()
- printScheme(), columns, dtypes, scheme
- createOrReplaceTempView()
- explain()

##### 5.5.2.3 비타입 트랜스포메이션 연산

**데이터의 실제 타입을 사용하지 않는 변환 연산**을 수행한다는 의미에서 붙여진 이름입니다.

동일한 Person 오브젝트로 구성된 RDD 와 데이터프레임을 준비하고 이들로부터 나이 값이 10 이상인 데이터만 조회할 경우 어떤 부분이 달라지는지 확인합니다.

다음처럼 RDD 의 경우, 정상 동작합니다.

```scala
scala > rdd.filter(p => p.age > 10).collect.foreach(println)
```

데이터프레임의 경우,

```scala
scala > df.filter(p => p.age > 10).show
```

위와 같이 하면, "value age is not a member of org.apache.spark.sql.Row" 오류가 납니다. 이는 filter 함수에서 사용된 변수 p 가 Person 타입이 아닌, org.apache.spark.sql.Row 타입의 오브젝트 라는 뜻으로, 이를 통해 데이터프레임의 filter 함수 내부에서 참조하는 데이터들이 원래 데이터 타입인 Person 이 아니라는 것을 알 수 있습니다. 

이처럼, 원래 데이터의 타입이 아닌 Row 타입을 사용해 트랜스 포매이션 연산을 수행하게 된다는 의미로 비타입 트랜스 포매이션 연산이라는 이름을 사용합니다.

다음은 기존 데이터 프레임에서 제공하던 연산에 해당하는 비타입 트렌스포매이션에 대한 것입니다.

- Row, Column, functions
- !==, ===
- alias(), as()
- isin()
- when()
- max(), mean()
- collect_list(), collect_set()
- count(), countDistinct()
- sum()
- grouping(), grouping_id()
- array_contains(), size(), sort_array()
- explode(), posexplode()
- current_date(), unix_timestamp(), to_date()
- add_months(), date_add(), last_dat()
- window()
- round(), sqrt()
- array()
- desc(), asc()

.....