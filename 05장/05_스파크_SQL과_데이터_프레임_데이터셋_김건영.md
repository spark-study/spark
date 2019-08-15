# 스파크 2 프로그래밍 정리

- ## 5장 스파크 SQL과 데이터프레임, 데이터셋
    0. ### 스파크 SQL
        - 스파크 SQL이란? = RDD에서 표현하지 못하는 스키마 정보를 표현가능하도록 보완된 모델.
        - 스파크 SQL(데이터 프레임, 데이터 셋)의 언어별 사용가능 유무

            스파크 SQL | 스칼라 | 자바 | 파이썬
            -- | -- | -- | --
            데이터 프레임 | O | X | O
            데이터 셋 | O | O | X

    1. ### 데이터 셋
        - 스파크 1.6에서 처음 소개된 데이터 모델이자 API.
        - SQL과 유사한 방식의 연산을 제공.
        - 데이터 프레임에선 되지 않은 컴파일 타임 오류 체크 가능.
    
    2. ### 연산의 종류와 주요 API
        - 연산 종류

            트랜스포메이션 연산 | 액션연산
            -- | --
            1. 타입 연산 = 원래의 타입을 사용한 연산<br> 2. 비타입 연산 = ROW타입을 사용한 연산 | 데이터셋 반환 X

        - 프로그래밍 구성요소

            구성요소 | 의미
            -- | --
            스파크 세션 | 1. 데이터 프레임을 생성하기 위해 이용.<br> 2. 인스턴스 생성을 위한 build()메서드 제공<br> 3. 하이브 지원 default
            데이터 셋 | RDD와 같은 타입 기반연산 + SQL과 같은 비타입 연산 제공
            데이터 프레임 | ROW 타입으로 구성된 데이터 셋
            DataFrameReader | 1. SparkSession의 read()메서드를 통해 접근 가능<br> 2. ["jdbc", "json", "parquet"] 등 다양한 input 제공.
            DataFrameWriter | 1. SparkSession의 write()메서드를 통해 접근 가능
            로우, 칼럼 | 데이터 프레임을 구성하는 요소(한 로우에 1개 이상의 칼럼 존재)
            functions | 데이터를 처리할떄 사용할 수 있는 각종 함수를 제공하는 오브젝트
            StructType, StructField | 데이터에 대한 스키마 정보를 나타내는 API
            GroupedData, GroupedDataSet | 1. groupBy() 메서드로 인해 사용<br> 2. 집계와 관련된 연산 제공

    3. ### 코드 작성 절차
        - 스파크 세션 생성
        - 스파크 세션으로부터 데이터셋 또는 데이터 프레임 생성
        - 생성된 데이터셋 또는 데이터프레임을 이용해 데이터 처리
        - 처리된 결과 데이터를 외부 저장소에 저장
        - 스파크 세션 종료
    
    4. ### 스파크 세션
        - 스파크 세션 사용 목적 = 
            1. 데이터 프레임, 데이터 셋 생성
            2. 사용자 정의 함수(UDF)를 등록 위함
        
        - 생성 예제
            ```python
            from pyspark.sql import SparkSession
            spark = SparkSession \
                .builder \
                .appName("sample")\
                .master("local[*]")\
                .getOrCreate()
            ```
        
        - 2.0 버전부터 하이브 기본 지원
            1. 기존 하이브 서버가 존재할 시 conf 디렉터리에 [hive-site.xml, core-site.xml, hdfs-site.xml] 파일 위치.

    5. ### 데이터프레임, 로우, 컬럼
        - RDD와의 차이 = 데이터 값뿐만이 아닌 스키마 정보까지 함께 다룬다는 차이 존재.
        - 데이터 프레임 생성 방법
            1. 외부의 데이터 소스
                - DataFrameReader의 read()메서드 사용
                - DataFrameReader가 제공하는 주요 메서드

                    메소드 | 의미
                    -- | --
                    format() | 1. input 데이터의 유형을 지정<br> 2. orc, libsvm, kafka, csv, jdbc, json, parquet, text, console, socket 등 사용 가능 <br> 3. 써드파티 라이브러리 존재
                    option/options() | 키와 값형태로 설정 정보 지정.
                    load() | input으로 부터 실제 데이터 프레임을 생성.
                    jdbc() | 데이터베이스를 input으로 사용하기위한 간편 메서드
                    json() | json을 input으로 사용하기 위한 간편 메서드(파일의 경우 각 라인 단위로 json이 구성되어야 함)
                    orc() | orc 형식 데이터 다룰수 있음
                    parquet() | parquet형식 데이터를 가져와 데이터 프레임 생성
                    schema() | 사용자 정의 스키마 지정가능
                    table() | 테이블 데이터를 데이터프레임으로 생성
                    text() | 텍스트 파일을 읽어 데이터 프레임 생성
                    csv() | 1. csv 파일을 읽어 데이터 프레임 생성<br> 2. 2.0버전부터 제공

            2. 기존 RDD 및 로컬 컬렉션으로부터 데이터 프레임 생성
                - RDD와 달리 스키마정보를 함께 지정하여 생성해야함.

                - 스키마 지정 방법
                    1. 리플렉션 = 스키마 정의를 추가하지 않고 데이터 값으로부터 알아서 스키마 정보를 추출하여 사용.
                    
                    2. 명시적 타입 지정 = 말 그대로 명시적으로 스키마를 정의하는것(StructField, StructType 사용)
                    
                    3. 이미지 파일을 이용한 데이터 생성
                        - 2.3부터 지원
                        - ImageSchema.readImages를 사용하여 생성 가능
                        - 이미지 파일 경로, 가로, 세로, 채널정보 등 추출 가능
                
        - 주요 연산 및 사용법
            1. 액션 연산
                - 연산 종류

                    연산 | 설명
                    -- | --
                    show() | 1. 데이터를 화면에 출력하는 연산 <br>2. 3개의 인자 존재(레코드 수, 표시할 값의 길이, 세로 출력 여부)<br> 3. 기본값은 20개 레코드수, 20바이트 길이 제한
                    head(), first() | 데이터셋의 첫번째 ROW를 반환.
                    take() | 첫 n개의 ROW 반환
                    count() | 데이터셋의 로우 갯수 반환
                    collect(), collectAsList() | 데이터셋의 모든 데이터를 컬렉션 형태로 반환(Array, List)
                    decribe() | 숫자형 칼럼에 대해 기초 통계값 제공(건수, 평균값, 표준편차, 최솟값, 최댓값)

            2. 기본 연산
                - 연산 종류

                    연산 | 설명
                    -- | --
                    cache(), persist() | 1. 데이터를 메모리에 저장.<br> 2. [NONE, DISK_ONLY, DISK_ONLY_2, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_ONLY_SER, MEMORY_ONLY_SER_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, OFF_HEAP]옵션 가능<br> 3. cache는 MEMORY_AND_DISK 옵션의 persist
                    createOrReplaceTempView() | 1. 데이터프레임을 테이블로 변환<br> 2. 스파크 세션인 종료되면 사라짐.
                    explain() | 데이터 프레임 처리와 관련된 실행 계획 정보를 출력.
            
            3. 비타입 트랜스포메이션 연산
                - 연산 종류

                    연산 | 설명
                    -- | --
                    alias(), as() | 칼럼이름에 별칭 부여
                    isin() | 칼러의 값이 인자로 지정된 값에 포함되어 있는지 여부 확인
                    when() | 1. if~else와 같은 분기 처리 연산 수행<br> 2. functions, Column 모두 제공(최초 사용시에는 functions의 메소드를 사용해야함)
                    max(), mean() | 칼럼의 최댓값, 평균값 계산.
                    collect_list(), collect_set() | 특정 칼럼을 모아서 list 혹은 set을 반환
                    count(), countDistinct() | 특정 칼럼에 속한 데이터의 갯수, 중복을 제거한 데이터의 갯수
                    sum() | 특정 칼럼 값의 합계
                    grouping(), grouping_id() | 소계 결과에 그룹화 수준을 파악하기 위해 사용.
                    array_contains(), size(), sort_array() | 특정값이 배열에 존재하는지, 배열의 사이즈, 배열 정렬 기능
                    explode(), posexplode() | 1. 배열, map 데이터를 여러개의 row로 변환<br> 2. posexplode의 경우 순서정보도 부여
                    current_data(), unix_timestamp(), to_date() | 1. current_data = 현재 시간값<br>2. unix_timestamp = string을 Date로 변환(yyyy-MM-dd HH:mm:ss)<br> 3. to_date = string을 Date로 변환(yyyy-MM-dd)
                    add_months(), date_add(), last_day() |  1. 달 더하는 연산<br> 2. 일 더하는 연산<br>3. 해당 달의 마지막 날짜.
                    window() | 1. DateType 칼럼을 대상으로 적용 가능. <br> 2. 일정크기의 시간을 기준으로 윈도우를 생성하여 각종 집계 연산 수행.
                    round(), sqrt() | 반올림, 제곱근 값 계산
                    array() | 여러 칼럼값을 배열로 만듬.
                    desc(), asc() | sort() 메소드와 함께 사용하는 정렬 방법 메소드
                    desc_nulls_first, desc_nulls_last, asc_nulls_first, asc_nulls_last | 정렬시 null값 기준 정의 메소드.
                    split(), length() | 문자열 분리, 길이 반환 메소드
                    rownum(), rank() | 오라클의 partitionBy개념의 Window 범위 안에서 사용하는 메소드
                    udf() | 1. 사용자 정의 함수<br> 2. 파이썬에서 자바 UDF사용하고 싶을 시 spark.udf.registerJavaFunction 메소드 사용
                    select(), drop() | 특정 칼럼을 포함/ 제외한 데이터 프레임 생성
                    filter(), where() | 특정 조건을 만족하는 데이터프레임만을 반환
                    agg() | 특정칼럼에 대해 sum, max와 같은 집합연산을 수행하기 위해 사용.
                    apply(), col() | 데이터프레임의 칼럼 객체 생성
                    groupBy() | SQL문의 groupBy 연산 수행, 집합연산 수행 가능
                    cube() | 데이터의 소계를 반환( = 소계대상인 칼럼들의 모든 가능한 조합의 부분합)
                    distinct(), dropDuplicates() | 1. distinct의 경우 모든 컬럼값이 같을때 중복 판단 <br> 2. dropDuplicates는 중복을 제거하고자 하는 컬럼 지정 가능
                    intersect() | 두개의 데이터프레임의 교집합
                    except() | 두개의 데이터프레임의 차집합
                    join() | 1. 두 개의 데이터프레임의 join<br> 2. inner, outer, left_outer, right_outer, leftsemi 등 join 종류 모두 지원<br>3. 조인 칼럼에 대해서는 중복으로 포함되지 않는 특징 존재
                    crossjoin() | 두 데이터프레임의 카테시안곱 지원
                    na() | 1. null, NaN값이 포함되는 경우 사용<br> 2. DataFrameNaFunctions 인스턴스 반환<br> 3. DataFrameNaFunctions가 가지고 있는 메서드 사용(drop, fill, replace 등등)
                    orderBy() | SQL의 orderBy와 같은 동작 수행
                    stat() | 1. 특정 칼럼값의 통계수치를 제공하는 DataFrameStatFunctions 인스턴스 제공<br> 2. corr, cov, crosstab, freqItems, sampleBy 등의 메소드 사용 가능
                    withColumn(), withColumnRenamed() | 새로운 칼럼 추가 혹은 이름을 변경하는 메서드
                    repartitionByRange() | repartition시 데이터 프레임의 갯수를 기준으로 균등하게 분포(2.3부터 가능)
                    colRegax() | 정규식을 이용하여 칼럼을 선택할 수 있는 기능(2.3부터 가능)
                    unionByName() | 1. union할 시 데이터프레임의 칼럼 이름을 기준으로 수행<br> 2. 그냥 union은 데이터프레임의 데이터 순서로 수행되기 때문에 데이터가 엉킬수 있음.
                    to_json, from_json | 칼럼 수준에서 데이터를 json으로 저장 및 가져오는 기능 제공.

            4. write()
                - 데이터프레임 데이터를 저장하기 위해 사용.
                - DataFrameWriter.write 사용
                - DataFrameWriter의 주요 메소드
                    
                    메소드 | 의미
                    -- | --
                    save | 데이터를 저장하는 메소드
                    format | 저장하는 데이터의 format을 지정.
                    partitionBy | 특정 칼럼값을 기준으로 파티션 설정
                    options | 데이터 저장시 추가 설정 시 사용
                    mode | 저장 모드 설정(Append, Overwrite, ErrorExists, Ignore)
                    saveAsTable | 테이블 형태로 저장 및 영구 저장 가능.
            5. Pandas 연동
                - PyArrow 라이브러리를 사용하여 스파크의 데이터프레임과 파이썬의 Pandas의 상호 변환 제공 -> 2.3버전부터 가능
                - spark.sql.execution.arrow.enabled 옵션을 true로 하여야 한다.

    6. ### 데이터 셋
        
        - 데이터 셋 사용 이유 = 데이터 개발 편의성과 컴파일 시 타입오류 검증이 가능(Row 클래스가 아닌 사용자 지정 클래스를 Type으로 사용하기 때문.)
        
        - 데이터 셋은 사용자 정의 타입 클래스를 사용하기 때문에 Spark SQL 최적화가 안될 수 있어, 성능면에서 악영향을 끼칠 수 있다.

        - 데이터 셋 생성
            1. 생성 방법 = 자바 객체, 기존 RDD, 데이터 프레임, 외부 source
            
            2. 생성 시 인코더 지정(필수) = 성능 최적화를 위해 기존 오브젝트를 스파크 내부 최적화 포맷으로 변환해야 하는데 때 사용(데이셋은 내부적으로 InternalRow클래스를 사용.)

            3. 스칼라의 경우, 기본타입의 경우에는 import를 통하여 암묵적 인코더 사용 가능.

            4. 자바의 경우, 데이터 프레임을 만든 후 이것을 데이터 셋으로 변환하여 사용해야 한다.

            5. 데이터 프레임으로부터 생성시에는 type을 지정해야하기 때문에 as() 메서드를 통하여 생성할 수 있다.

            6. range() 메서드를 통하여 간단한 샘플데이터 또한 생성 가능.

            
        - 타입 트랜스포메이션 연산
            - 연산 종류
                
                연산 | 설명
                -- | --
                select() | 1. 데이터 프레임의 select 메서드와 동일<br> 2. 단, as() 메서드를 통해 type을 지정해야함.
                as() | 1. 데이터 셋에 별칭 부여<br> 2. column에 부여하는 것이 아닌 데이터 셋에 타입을 부여.
                distinct() | 중복을 제외한 요소만으로 데이터셋 반환
                dropDuplicates() | distonct() 메서드에서 칼럼을 지정할 수 있는 기능 추가.
                filter() | 사용자 정의 조건을 만족하는 데이터만으로 구성된 데이터 셋 반환.
                map(), flatMap() | 1. RDD의 map(), flatMap() 메서드와 동일<br> 2. 단 데이터 셋의 타입을 인자로 사용 가능.<br> 3. 자바의 경우 인코더 지정 필수
                groupByKey() | 1. RDD의 groupBy와 기능 동일<br> 2. KeyValueGroupedDataset 클래스 반환
                agg() | 1.  데이터 프레임에서 본 agg() 메서드와 동일<br> 2. 단, 사용하는 칼럼은 반드시 TypeedColumn 클래스여야 한다.<br> 3. 사용가능한 집계 연산 = avg(), count(), sum(), sumLong()
                mapValues(), reduceGroups() | 1. KeyValueGroupedDataset클래스의 매서드<br> 2. 맵연산, 리듀스 연산을 수행.
        
        - 하이브 연동
            
            1. 스파크에서는 별도의 하이브 서버없이 테이블로 save 및 load 가능(-> 자체적으로 DB, 테이블관련 메타 데이터 저장.)
            2. 기존의 하이브 서버와 연동할 시 conf 디렉터리 하위에 hive-site.xml, core-site.xml, hdfs-site.xml 파일을 복사하여야 한다.
        
        - 분산 SQL 엔진
            
            1. 실행 방법으로는 bin 디렉터리 하위 start-thriftserver.sh 실행
            2. 하이브 클라이언트인 beelin 수행시 스파크 SQL을 사용하는것으로 인해 하이브 클라이언트 수행하더라도, 엔진은 스파크를 사용한다는것을 알 수 있음
        
        - SparkSQL CLI
            1. SQL 관련하여 메타데이터만을 사용하는 것이라면 간단하게 제공하는 기능
            2. 실행방법으로는 bin 디렉터리의 spark-sql 실행.

        - 쿼리 플랜과 디버깅
            1. 스파크 세션, 스파크 스테이트, 스파크 컨텍스트
                
                용어 | 정의
                -- | --
                스파크 컨텍스트 | 스파크가 동작하기 위한 각종 백엔드 서비스에 대한 참조를 가지고 있는 객체.
                세션 스테이트 | 스파크 세션의 상태 정보
                스파크 세션 | 스파크 컨텍스트에 세션 상태 정보를 추가로 담은 것
            
            2. 스파크 세션은 최적화와 관련된 기능을 위해 사용하는 각종 메타 정보, 상태 정보를 가지고 있다.

        - QueryExecution
            
            1. Spark SQL 사용시 최적화 과정에서 일어나는 일들을 확인할 수 있는 API.
            2. QueryExecution에서 제공하는 실행 계획은 아래에서 위로 보여준다.
            3. Query 실행은 아래와 같은 순으로 진행되며 최적화가 된다.
                - LogicalPlan 생성(QueryExecution.logical 메서드로 보이는 계획)
                - SessionState의 Analyzer가 적용된 LogicalPlan 생성(QueryExecution.analyzed 메서드로 보이는 계획)
                - SessionState의 Optimizer가 적용된 LogicalPlan 생성(QueryExecution.optimizedPlan 메서드로 보이는 계획)
                - SessionState의 SparkPlanner가 적용된 
                SparkPlan 생성(QueryExecution.sparkPlanner 메서드로 보이는 계획)
                - SparkPlan에 추가적인 최적화 과정 적용하여 SparkPlan 생성(QueryExecution.executedPlan 메서드로 보이는 계획)
            
        
                
                

                






