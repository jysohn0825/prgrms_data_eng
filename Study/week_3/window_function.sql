/*
-- 사용자별로 처음 채널과 마지막 채널 알아내기
*/

-- 노가다로 하는 방법 ( 나열해서 처음과 끝을 본다 )
SELECT ts, channel
FROM raw_data.user_session_channel usc
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
WHERE userid = 251
ORDER BY 1

-- 위에서 한 것에 일련번호를 붙이는 것
SELECT
    ts, channel, ROW_NUMBER() OVER (partition by userid order by ts) as N FROM raw_data.user_session_channel usc
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
WHERE userid = 251;

SELECT * FROM ( -- FROM 다음에 ()로 묶을 수 있다
  SELECT ts, channel, ROW_NUMBER() OVER (partition by userid order by ts) as N-- partition by는 userid가 같은 것끼리 일련번호를 붙이고 order by는 ts의 오름차순 기준으로 

  FROM raw_data.user_session_channel usc
  JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
  WHERE userid = 251
)
WHERE N = 1 -- 1대신 251을 넣고 ts를 내림차순으로 바꾸면 마지막을 볼 수 있다