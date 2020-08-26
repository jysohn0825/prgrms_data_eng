/*********************************************************
-- 매달 Active 한 사용자 선별하는 쿼리
*********************************************************/
SELECT
 
  -- LEFT(ts, 7) or DATE_TRUNC(‘month’, ts) 와 같은 의미이고 DATE_TRUNC는 timestamp, to_char는 string 리턴
  to_char(st.ts, 'YYYY-MM') as Monthly, 
  count(DISTINCT usc.userid) as ActiveUser
FROM
  raw_data.session_timestamp as st
  JOIN raw_data.user_session_channel as usc 
    ON st.sessionid = usc.sessionid
GROUP BY 
  1         /* select의 첫번째 인자를 기준으로 group by, order by */
ORDER BY
  1
