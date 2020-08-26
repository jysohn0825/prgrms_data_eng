/*********************************************************
-- 매달 Active 한 사용자 선별하는 
*********************************************************/
SELECT
  
  /*LEFT(ts, 7) or DATE_TRUNC(‘month’, ts
    date_trunc는 timestamp를 리턴 to_char는 string 리턴
  */
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
