/*********************************************************
-- Assignment - 2
-- Gross Revenue가 가장 큰 UserID 10개 찾기
-- Gross revenue: Refund 포함한 매출
*********************************************************/

SELECT userID, SUM(amount)
FROM raw_data.session_transaction AS st
JOIN raw_data.user_session_channe ASl usc ON st.sessionid = usc.sessionid
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
