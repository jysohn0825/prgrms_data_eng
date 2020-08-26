/*********************************************************
-- 채널별 월 매출액 테이블 만들기
-- session_timestamp, user_session_channel, channel, session_transaction 테이블들을 사용
-- channel에 있는 모든 채널에 대해 구성해야함 (값이 없는 경우라도)
-- 아래와 같은 필드로 구성
-- month
-- channel
-- uniqueUsers (총방문 사용자)
-- paidUsers (구매 사용자: refund한 경우도 판매로 고려)
-- conversionRate (구매사용자 / 총방문 사용자)
-- grossRevenue (refund 포함)
-- netRevenue (refund 제외)
*********************************************************/

SELECT LEFT(ts, 7) "month",
       channelname,
       COUNT(DISTINCT userid) uniqueUsers,
       COUNT(DISTINCT CASE WHEN amount > 0 THEN userid END) paidUsers,
       ROUND(paidUsers::decimal*100/NULLIF(uniqueUsers, 0),2) as conversionRate,
       SUM(amount) grossRevenue,
       SUM(CASE WHEN refunded is False THEN amount END) netRevenue
FROM raw_data.channel c
LEFT JOIN raw_data.user_session_channel usc ON c.channelname = usc.channel
LEFT JOIN raw_data.session_transaction st ON st.sessionid = usc.sessionid
LEFT JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
GROUP BY 1, 2
ORDER BY 1, 2;
