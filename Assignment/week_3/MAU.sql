/*********************************************************
-- 월별 코호트 MAU (Monthly Active User) 테이블 만들기
-- cohort 분석의 목적 - 고객의 Retention(유지)을 분석 <-> 반대는 Churn (이탈) 분석
-- 정답 테이블은 아래와 같이 구성되어야 한다
-- cohort moth	1	2	3	4	5	6	7
-- 2019-05-01	281	262	237	229	224	213	206
-- 2019-06-01	197	175	160	150	148	145
-- 2019-07-01	211	189	175	167	155
-- 2019-08-01	84    73    71	69
-- 2019-09-01	17	14	13
-- 2019-10-01	150	124
-- 2019-11-01	9
*********************************************************/

-- method 1
SELECT cohort_month, visited_month, COUNT(DISTINCT cohort.userid)
FROM (
      SELECT userid, MIN(LEFT(ts, 7)) cohort_month
      FROM raw_data.user_session_channel usc
      JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
      GROUP BY 1
) cohort
JOIN (
      SELECT DISTINCT userid, LEFT(ts, 7) visited_month
      FROM raw_data.user_session_channel usc
      JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
) visit ON cohort.cohort_month <= visit.visited_month and cohort.userid = visit.userid
GROUP BY 1, 2
ORDER BY 1, 2;



--method 2
SELECT cohort_month, DATEDIFF(month, cohort_month, visited_month)+1 month_N, COUNT(DISTINCT cohort.userid)
FROM (
    SELECT userid, MIN(DATE_TRUNC('month', ts)) cohort_month
    FROM raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
    GROUP BY 1
) cohort
JOIN (
    SELECT DISTINCT userid, DATE_TRUNC('month', ts) visited_month
    FROM raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
) visit ON cohort.cohort_month <= visit.visited_month and cohort.userid = visit.userid
GROUP BY 1, 2
ORDER BY 1, 2;

