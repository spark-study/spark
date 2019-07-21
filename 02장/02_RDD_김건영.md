# 스파크 2 프로그래밍 정리

- ## 2장 RDD
    1. 스파크 컨텍스트란?
        - 스파크 애플리케이션과 클러스터의 연결을 관리하는 객체.
        - RDD를 생성하기 위해 필요.
        - 기본적으로 대신자의 역할을 수행하는것을 컨텍스트라고 하기에 위의 일들을 하는것은 당연한것 같다.
    2. 스파크 함수 작성 시 유의사항
        - 스파크는 functional 컨셉을 기반으로 하고 있고, RDD 역시 트렌스포메이션 연산을 수행하면 새로운 RDD를 만들어 낸다.<br>
        그러므로 순수함수의 형태로 작성을 해야하며 그렇지 않으면 직렬화 문제에 직면하게 된다.
    3. RDD 액션( tm=트랜스포메이션 연산, a=액션연산)
        - collect = RDD 데이터를 배열로 반환하는 a연산
        - count = RDD 데이터의 갯수를 반환하는 a연산
        - map = RDD에 있는 데이터에 지정 연산을 수행한 후 RDD를 반환하는 tm 연산
        - flatMap = map연산과 비슷하지만 반환 타입이 iterate여야하는 tm 연산
        - mapPartitions = map연산을 파티션 단위로 하는 tm 연산
        - mapPartiitonsWithIndex = mapPartitions에서 각 파티션의 index정보도 같이 넘기는 tm 연산
        - mapValues = (key, value) 형태의 RDD에만 적용가능하며 value에만 map 연산을 수행 후 RDD를 반환하는 tm연산
        - flatMapValues = RDD가 (key, value)형태일때 위의 flatMap 연산을 value에 적용하고자 할때 사용하는 tm 연산
        - zip = 2개의 RDD를 (key, value)로 묶어주는 tm 연산.<br>
        RDD의 데이터 크기가 같아야 한다.
        - zipPartitions = 파티션 단위로 zip연산을 수행하는 tm 연산.<br>
            - zipPartitions연산은 zip 연산과 다르게 최대 4개까지 지정 가능
            - 파이썬에서는 사용 불가.
            - 파티션의 갯수가 같아야 함.
        - groupby = RDD의 데이터를 지정한 group 집합으로 묶어 RDD를 반환하는 tm 연산
        - groupbyKey = (key, value) 형태의 RDD를 키 기준으로 group 하여 반환하는 tm 연산
        <br> =  (key, sequence[value])로 구성.
        - cogroup = (key, value) 형태의 여러 RDD를 key 기준으로 group 한 후 각 RDD의 sequence[value]를 Tuple로 반환하는 tm 연산
        <br> ex) = (키, Tuple(RDD1의 sequence[value], RDD2의 sequence[value]))
        <br> 최대 3개까지 RDD를 group 할 수 있다.
        - distinct = RDD에서 중복을 제거한 뒤 RDD를 반환하는 tm 연산
        - certesian = 2개 RDD의 카테시안 곱을 한 RDD를 반환하는 tm 연산(M x N)
        - subtract = 2개 RDD에서 1개의 RDD값을 제외한 값들을 반환하는 tm 연산(M - N)
        - union = 2개 RDD의 합집합한 결과를 반환하는 tm 연산(M + N)
        - intersection = 2개의 RDD의 교집합을 반환하는 tm 연산 (M ∩ N)
        - join = 2개의 (key, value) 형태의 RDD를 키 기반으로 join하여 RDD를 반환하는 tm 연산
            - ex ) (키, Tuple(첫번째 RDD의 요소, 두번째 RDD의 요소))
            - join은 inner join이다. 한마디로 join이 되지 않으면 반환 RDD에 포함 X
        - leftOuterJoin, rightOuterJoin = sql의 left(right)OuterJoin과 비슷, tm 연산
            - 조인결과가 없을 수도 있어, 주최 RDD가 아니라면 Optional 값으로 반환.
            - ex) (a, (1, None)), (b, (1, Some(2))), (c, (1, Some(2)))
        - subtractByKey = 2개의 (key, value) 형태의 RDD에서 같은 key를 제외하고 RDD를 반환하는 tm 연산.
        - reduceByKey = 2개의 RDD에서 같은 key로 병합하여 RDD를 반환하는 tm 연산.
        - foldByKey = reduceByKey 연산에서 초기값을 부여하는 옵션이 추가된 tm 연산.
        - combineByKey = 반환 RDD의 값 타입이 변경될수 있는 tm 연산
            - 사용법 = 
                1. 첫번째 인자(초기값을 위한 함수)
                2. 두번째 인자(RDD의 각 파티션에서 수행할 함수)
                3. 세번째 인자(각 파티션들을 결합하는 함수)
        - aggregateByKey = combineByKey연산의 첫번째 인자가 함수가 아닌 값으로만 변경된 tm 연산.
        - pipe = map연산에서 외부 프로세스를 사용하는 tm 연산.
            - ex) rdd.pipe("cut -f 1,3 -d ,") -> cut 유틸 사용.
        - coalesce, repartition = 두 연산 모두 RDD의 파티션 크기를 조정하는 연산.
            - coalesce = 파티션 갯수를 줄이기만 가능
            - repartition = 파티션 갯수를 늘리고 줄이기 모두 가능
            - repartition은 무조건 셔플 발생, coalesce은 지정했을때만 셔플 발생.
            - 셔플 연산은 비용이 큰 연산이니 고려하여 사용하여야함.
        - repartitionAndSortWithinPartitions = 파티션 갯수 조절 후 각 파티션에서 정렬한 뒤 RDD를 반환하는 tm 연산.
        - partitionBy = RDD의 값들을 특정 파티션으로 옮기고 싶을때 사용하는 tm 연산
            - 개인적으로 파티션은 hdfs의 block disk와 같이 각 Slave에 분산되어 있는것으로 판단되기 때문에 이 연산은 큰 비용의 연산이라고 생각이 든다.
            - 대신 카프카의 키 파티셔닝을 하는 것과 같이 특정 조건에서는 더욱 이익을 볼 수도 있기 때문에 잘 고려하여 사용해야 한다고 판단된다.
        - sortByKey = 키 값을 기준으로 정렬한 후 RDD 를 반환하는 tm 연산.
        - keys, values = 자바 map의 keys, values와 같은 의미의 tm 연산
        - sample = 이건 이해가 잘 안가 질문하고 싶은 연산입니다..ㅠ
        - first = RDD의 첫번째 요소를 반환하는 a 연산
        - take = RDD의 첫번째 요소부터 n개를 반환하는 a 연산
        - takeSample = sample 연산이 이해가 안가.... 이것도...잘..
        - countByValue = 값을 각 카운팅하여 map으로 반환하는 a 연산
            - [1, 1, 2, 3, 3] => Map({1:2, 2:1, 3:2})
        - reduce = 2개의 RDD요소를 하나로 합치는 a 연산
            - 입력과 출력의 타입이 같아야함.
        - fold = reduct 연산에서 초기값을 지정할 수 있는 a 연산
            - 입력과 출력의 타입이 같아야함.
        - aggregate = reduce와 fold를 합친 a 연산
            - 사용법 = 
                1. 첫번째 인자(초기값)
                2. 두번째 인자(RDD의 각 파티션에서 수행할 병합함수)
                3. 세번째 인자(각 파티션들을 결합하는 함수)
            - 입력과 출력의 타입이 달라도 됨.
        - sum = RDD의 요소의 합을 반환하는 a 연산
            - int, double, long 등 숫자 타입의 RDD에서만 가능
        - forEach, forEachPartition = map, mapPartition과 기능은 동일하나 반환값이 없는 점이 다른 a 연산
            - 이 연산은 각 개별 노드에서 수행됨.
        - toDebugString = RDD의 세부 정보를 파악하기 위한 a 연산
        - cache, persist, unpersist = 
            - cache, persist는 첫 액션연산 후 RDD 정보를 메모리 또는 디스크에 저장하는 메소드.
            - unpersist는 필요없는 데이터를 캐시에서 제거할때 사용.
            - cache, persist는 정보를 메모리에 올리게 되면 빠른 연산을 할 수 있게 도와줌. 그러나 너무 많이 올리게 되면 GC대상이 되어 악영향을 줄 수도 있으니 이 메서드도 함부로 남발해서는 안됨.
    4. RDD에서 데이터 불러오기 및 저장하기
        - 텍스트 파일(파이썬 기준)
            - 불러오기 = textFile("path", {"partition 갯수"}) 메서드 사용.
            - 저장하기 = saveAsTextFile("path", {"압축 방식"}) 메서드 사용.
        - 오브젝트 파일(파이썬 기준)
            - 불러오기 = pickleFile("path")
            - 저장하기 = saveAsPickleFile("path")
            - 오브젝트 파일의 경우 load시 저장된 RDD 타입과 같게 해야한다.
        - 시퀀스 파일(파이썬 기준) = (key, value) 형식의 데이터 파일.
            - 불러오기 = newAPIHadoopFile("path", "inputformatClass", "keyClass", "valueClass")
            - 저장하기 = saveAsNewAPIHadoopFile("path", "outputformatClass", "keyClass", "valueClass")
            - 시퀀스 파일의 경우 하둡에서 자체적으로 정의한 직렬화 프레임워크를 사용. -> 즉 시퀀스 파일의 데이터는 하둡의 Writable이 구현되어져 있어야 한다.
    5. 클러스터 환경에서의 공유 변수
        - 분산 환경으로 인해 다수 프로세스들이 공유할 수 있는 읽기 자원과 쓰기 자원이 필요.
        - 스파크에서는 이를 지원하기위해 브로드캐스트와 어큐물레이트를 제공.
        - 브로드 캐스트 = 스파크 잡이 실행되는 동안 모든 서버에서 공유가능한 읽기 자원.
            - 브로드 캐스트 변수 지정방법.
                1. 공유하고자 하는 변수를 오브젝트로 생성
                2. sc의 broadcast()를 사용하여 변수 세팅
                3. 2번의 반환값에서 .value 사용하여 공유 변수 접근 가능.
        - 어큐뮬레이터 = 브로드 캐스트와 달리 각 서버에서 공유가능한 쓰기 자원.
            - 어큐뮬레이터는 각 분산 서버에서 로깅 집중화 등 한곳에서 정보를 파악하기 위해 사용해야함.
            - 어큐뮬레이터 사용법(파이썬 기준)
                1. sc.accumulate("int / float 값") 메서드를 통해 생성
                2. accumulate.add 연산을 사용하여 write 함.