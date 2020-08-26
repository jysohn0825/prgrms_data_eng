/*
-- session이 가장 많이 생성되는 시간대?
-- 만일 세션이 가장 많이 생성되는 요일을 알고 싶다면 EXTRACT(DOW FROM ts)
-- 0부터 6사이의 값이 리턴: 0은 일요일, … 6이 토요일
*/

SELECT EXTRACT(HOUR FROM st.ts), COUNT(DISTINCT(usc.userid))
FROM raw_data.user_session_channel usc 
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
GROUP BY 1
ORDER BY 2 DESC;