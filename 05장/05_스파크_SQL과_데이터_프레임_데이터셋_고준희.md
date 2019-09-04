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

### 5.6 데이터셋

RDD 와 데이터프레임 이외에 다른 데이터 모델을 제시하는 가장 큰 이유는, 데이터프레임과 RDD 간의 데이터 타입을 다루는 방식의 차이때문입니다.

RDD 는 내부 데이터의 타입을 명확하게 정의해서 사용하도록 강제돼있는데 반해, **데이터프레임의 경우 내부 데이터가 Row 의 집합이라는 것만 보장돼 있을 뿐 실제 데이터의 타입에 대한 정보는 외부에 노출되지 않습니다**.

데이터프레임은 데이터셋과 다르지 않은 완전히 동일한 클래스입니다. 즉, 동일한 데이터를 서로 다른 방식으로 표현하기 위한 모델이지 서로 다른 것이 아닙니다. 실제로 데이터프레임과 데이터셋은 자유롭게 변환이 가능하며 둘 중 어떤 것을 사용하더라도 내부적인 구현은 동일한 방법을 따릅니다.

단, 데이터셋을 사용할 경우 스파크 내부에서 최적화가 가능한 스파크 내장 함수보다 사용자 정의 함수나 스칼라 또는 자바 언어의 다양한 외부 라이브러리를 사용하게 될 가능성이 높은데, 이 경우 자칫 성능엥 안 좋은 영향을 줄 위험이 있으므로 주의해서 사용해야 합니다.

#### 5.6.1 데이터셋 생성

자바 객체 또는 기존 RDD, 데이터프레임으로부터 생성될 수 있습니다.

##### 5.6.1.1 파일로부터 생성

외부 파일로부터 데이터셋을 생성하기 위해서는 스파크세션이 제공하는 read() 메서드를 이용합니다.

##### 5.6.1.2 자바 객체를 이용해 생성

앞에서 살펴본 리플렉션 방식의 데이터프레임 생성과 유사합니다.

```java
Person row1 = new Person("hayoon", 7, "student");
Person row2 = new Person("jko", 12, "teacher");
Person row3 = new Person("junhee", 14, "worker");
Person row4 = new Person("park", 34, "fireman");

List<Person> data= Arrays.asList(row1,row2,row3,row4);
Dataset<Person> df2 = spark.createDataset(data, Encoders.bean(Person.class));
```

**데이터셋을 생성할 때는 인코더 정보를 반드시 명시**해야합니다. 인코더는 자바 객체와 스파크 내부 바이너리 포맷 간의 변환을 처리하기 위한 것으로 스파크 1.6 에서 데이터셋과 함께 처음 소개되었습니다. 인코더가 하는 역할은 기존 자바 직렬화 프레임워크나 Kyro 같이 자바 객체를 바이너리 포맷으로 변환하는 것입니다. 

하지만, 기존 직렬화 프레임워크처럼 단순히 네트워크 전송 최적화를 위한 바이너리 포맷을 만드는 것에 그치는 것이 아니라, 데이터의 타입과 그 데이터를 대상으로 수행하고자 하는 연산, 데이터를 처리하고 있는 하드웨어 환경까지 고려한 최적화된 바이너리를 생성하고 다룬다는 점에서 차이가 있습니다.

##### 5.6.1.3 RDD 및 데이터프레임을 이용해 생성

```scala
import spark.implicits._ // 스칼라의 경우, 기본 타입에 대한 인코더 정보를 암묵적 변환 방식을 이용해 제공

val rdd = sc.parallelize(List(1,2,3))
val ds3 = spark.createDataset(rdd)
val ds4 = rdd.toDS()
```

```java
JavaRDD<Person> rdd= sc.parallelize(data);
Dataset<Row> df3 = spark.createDataFrame(rdd, Person.class);
Dataset<Person> ds4 = df3.as(Encoders.bean(Person.class));
```

##### 5.6.1.4 range() 로 생성

스파크세션이 제공하는 range() 메서를 이용해 연속된 숫자로 구성된 간단한 데이터셋을 생성할 수 있습니다.

#### 5.6.2 타입 트랜스포매이션 연산

데이터셋이 제공하는 연산은

- 기본연산
- 타입/비타입 트랜스포메이션 연산
- 액션 연산

타입 트렌스포메이션 연산으로는

- select
- as
- distinct
- dropDuplicates
- filter
- map, flatMap
- groupByKey()
- agg()
- mapValues, reduceGroups
- ...

### 5.7 하이브 연동

하이브는 SQL 기반의 데이터 처리 기능을 제공하는 오픈소스 라이브러리 프로그램 작성의 편리함과 HiveQL 이라고 하는 강력하고 다양한 기능의 쿼리 API 로 인해 널리 사용되고 있는 데이터웨어하우스 시스템입니다.

스파크SQL 은 하이브 시스템을 사용하지 않더라도 표준 SQL 이외에 하이브QL 을 사용할 수 있도록 지원하며, 필요한 경우 외부 하이브 시스템과 직접 연동해서 테이블을 공유하고 데이터를 주고 받는 방법도 제공합니다.

별도로 설치한 하이브 서버가 있고 그 서버와 연동해서 사용하도 싶다면 스파크홈의 conf 디렉토리 아래에 하이브와 하둡 설정 파일인 hive-site.xml , core-size.xml , hdfs-site.xml 파일을 복사하며 됩니다.

### 5.8 분산 SQL 엔진

스파크SQL 은 그 자체가 분산 SQL 서버로 동작해서, 다른 프로그램이 JDBC 또는 ODBC 방식으로 접속한 뒤 스파크에서 생성한 테이블을 사용할 수 있는 기능을 제공합니다. 따라서 이 기능을 사용하면 스파크 프로그램을 작성하지 않더라도 스파크 테이블을 대상으로 쿼리를 수행할 수 있습니다.

스파크 SQL 엔진을 실행한 뒤 하이브의 JDBC 클라이언트 프로그램인 beeline 을 이용해 테이블을 조회할 수 있습니다.

### 5.9 Spark SQL CLI

스파크가 제공하는 명령행 툴입니다.

### 5.10 Query Plan 과 디버깅

RDD 라는 훌륭한 API 가 있음에도 데이터프레임/데잉터셋 이라는 새로운 API 가 등장하게 된것은 데이터를 처리하는 과정의 최적화를 통한 성능 개선 가능성 때문입니다.

#### 5.10.1 SparkSession / SessionState / SparkContext

스파크세션 객체 안에는 스파크 컨텍스트 객체가 포함되어 있기 때문에, RDD 를 만들 때나 데이터프레임을 만들 때나 상관없이 스파크 세션 객체를 먼저 생성하면 됩니다.

스파크세션 객체를 만들려면 스파크컨텍스트를 비롯한 SessionState 객체가 먼저 생성되어 있어야하며, 실제로 어딘가에 이부분을 처리하는 코드가 있습니다. 

**즉, 스파크세션은 스파크컨텍스트에 세션상태 정보(SessionState) 를 추가로 담은 것이라고 할 수 있습니다.**

그렇다면, 스파크세션에 세션 상태 정보라는 것을 추가한 이유는 ? 스파크 컨텍스트는 스파크가 동작하기 위한 각종 백엔드 서비스에 대한 참조를 가지고 있는 객체이기 때문입니다.

스파크에서 동작하는 모든 어플리케이션은 백엔드 서버와 통신하기 위해 스파크컨텍스트 객체를 사용해야하며 이 같은 이유로 스파크센션의 경우에도 스파크컨텍스트를 먼저 생성한 후 이에 대한 참조를 내부적으로 유지하고 있는 것입니다.

#### 5.10.2 QueryExecution

데이터프레임을 사용할 경우 스파크가 내부적으로 최적화 과정을 거쳐 실제 실행 코드를 생성합니다. 스파크에서는 이런 변환 과정에서 일어나는 일을 사용자가 확인할 수 있게 특화된 API 를 제공하는데 대표적인 것이 queryExecution 메서드 입니다.

#### 5.10.3 LogicalPlan 과 SparkPlan

스파크는 최적화 쿼리를 수행할 때 논리적 실행 계획을 나타내는 LogicalPlan 과 구체적 실행 계획을 나타내는 SparkPlan 이라는 두개의 클래스를 사용합니다. 