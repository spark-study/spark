# 5.5 데이터 프레임, 로우, 칼럼
	-RDD가 데이터의 값을 다루는 데 초점
    - DataFrame은 데이터 값 뿐만 아니라 데이터에 대한 스키마 정보 까지

### 1. 데이터 프레임
#### 1. 생성
		1) read
        2) format 지정
        3) option 추가
        4) load
        
1. 스키마
스키마 지정 방법은 2가지
1) 자동 - createDataFrame
2) 지정 : 스키마 추논 시간 줄이기 위해 - structField, structType


### 2. 주요 연산 및 사용 법
	-DataSet: dataset[int], dataset[string]
    -DataFrame: dataset[row], dataset type이 row 인 데이터 셋.
    구분해서 사용하는 이유 : 트렌스포메이션 연산의 종류가 달라짐.
    
데이터 프레임에 특화된 연산들을 알아보자
#### 1. Action 연산
##### (1) Data 보기
- show(n, bool - col 내용 길면 줄이기)
- head, first - 1개 보여줌
- take(n) - n개 보여줌
- collect - 전부 보기 ( [ ],[ ],[ ]), collectAslist(python 지원 안됨) - ( [ [ ],[ ],[ ] ] )

##### (2) 연산
- count - row 수 return
- describe - 평균, 표준편차, 최대, 최솟값 포함하는 데이터 프레임 return
	
#### 2.1 기본 연산
dataset 연산
	- 기본 연산 : 둘 다 가능
    - 티입 transformation : only dataset, dataframe X
    - 비타입 transformation : only dataframe

- cache(), persist()
 	persist()는 데이터 저장 방법과 관련된 옵션 선택 가능
- 스키마 정보 조회
  - printSchema(),columns,dtypes,chema
- createOrReplaceTempView()
	- 데이터 프레임을 테이블처럼 sql 사용해서 처리 가능하게
- **explain()**
  - 실행 계획 정보 출력
    
#### 2.2 비타입 transformation : 실제 타입을 사용하지 않는 변환 연산 수행
** row, column, funtion api 함수 알아보자**
##### (1) column class
- 비교 연산자
	- 두개의 칼럼 값 비교 "=====" or "!===="
- column 이름 
	- 	alias(), as() 	
- isin()
- when()
    
##### (2) function class
-	max, min
-	**collect_list, collect_set**
	-	특정 칼럼 값을 list나 set으로 모음
- count, countDistinct
- sum
- grouping, grouping_id
	- grouping_id 는 group 연산 수준을 보여줌
- array_contains, size, sort array
    - 배열 타입의 칼럼에 사용
- **explode, posexplode**
    - 하나의 배열 칼럼에 포함된 요소를 여러개의 행으로 변환, posexplode는 위치 정보를 함께 반환
- current_date, unix_timestamp,to_date
- add_month, date_dadd, last_day
`- 날짜 연산
- **window**
    - DateType에 적용 가능, 일정 크기의 시간 윈도우 생성 -> 집계 연산을 위해
- round, sqrt
- array
    - 여러개의 칼럼 값을 한의 배열로 만듬
- desc, asc
- split, length
- **rownum, rank**
    - 297p
    - 데이터를 몇개의 윈도우로 구분할 수 있음. 일정 그룹 기중에 다른 부분집합 집계에 사용
- **udf : User Define Function**
- select, drop
- filter, where
- agg
    - 특정 칼럼에 대해 집합 연산 수행 min, max 등
    - return type이 dataFrame
- apply, col
    - column 생성
- alias, as
- **groupby**
    - 집합 연산 생성하는데, **pivot**, count, sum 등
- **cube**
    - 각 부분 집합에대한 결과 보여줌 sum 등
- distinct, dropDuplicates
- drop
- intersect
- except
- join
    - ** intersect 와 inner join 차이 : intersect는 모든 컬럼 대상, inner join은 특정 컬럼들 대상 **
- crossjoin
    - 카테시안 곱
- na
    - null 처리
- orderby
- rollup
    - ** cube, rollup, group by 차이 **
   	 	- group by : 1차
   	 	- rollup : group by 상세
   	 	- cube : cross tab에 대한 summary 포함
- stat
    - 자주 사용하는 통계값
- withColumn
    - 새로운 칼럼 추가
- with ColumnRename
    - 기존 칼럼 이름 변경
- **write**
    	1) format
        2) partition
        3) option
        4) mode
        5) save
   
# 5.6 데이터 셋
- 데이터 프레임과 RDD가 있는데 다른 데이터 모델을 제시하게 된 가장 큰 이유는 **데이터 프레임과 RDD간의 데이터 타입을 다루는 방식의 차이** 때문이다.
- RDD는 데이터 타입을 명확하게 정의해서 사용하는 것이 강제돼 있는 반면에, 데이터 프레임의 경우 내부 데이터가 row의 집합이라는 것만 보장 돼 있을 뿐, 실제 데이터 타입에 대한 정보는 외부에 노출되어 있지 않음.
- 이 이유로, 변수 할당 관련 문제 뿐만 아니라 코드를 작성하는 과정에서도 칼럼명이나 타입을 실수로 잘못 다루더라도 컴파일러나 IDE가 이를 바로 잡아낼 수 없음.
- 따라서 데이터프레임이 가진 단저을 보완하면서도 스키마 정보를 활용한 성능 최적화까지 제공할 수 있는 새로운 모델, 즉 RDD와 데이터프레임이 가진 장점을 모두 포용할 수 있는 모델이 필요해 졌고 그것이 ++데이터 셋++

#### 1. 데이터셋 생성
자바 객체, 기존 RDD, 데이터프레임으로부터 생성 가능
#####(1) 파일로 부터 생성
- read

#####(2) 자바 객체로부터 생성
- createDataset

#####(3) RDD, 데이터프레임을 이용해 생성
- createDataset(rdd)
- rdd.toDS

#####(4) range()로 생성
- 연속된 숫자로 구성된 데이터셋 생성 가능

#### 2. 타입 트렌스포메이션 연산
RDD, 데이터 프레임 연산 사용 가능. 특이한 것만 기록
- dropDuplicates
- reduceGroup
	- groupbykey와 비슷

# 5.7 hive 연동
- hive read, write 가능
- sql 사용 가능

# 5.8 분산 SQL 엔진
- spark 테이블을 대상으로 sql쿼리 수행가능

# 5.9 Spark SQL CLI
- 단순히 Hive처럼 메타스토어 서비스만을 사용하는 것이 목적이라면, 스파크가 제공하는 도다른 명령어 툴인 Spark SQL CLI 사용 가능

# 5.10 정리



            







