{
          'table': 'user_summary',
          'schema': 'analytics',
          'main_sql': """SELECT c.channelname, usc.sessionid, usc.userid, sts.ts, st.refunded, st.amount FROM raw_data.channel AS c
LEFT JOIN raw_data.user_session_channel AS usc ON c.channelname = usc.channel
LEFT JOIN raw_data.session_timestamp AS sts ON usc.sessionid = sts.sessionid
LEFT JOIN raw_data.session_transaction AS st ON usc.sessionid = st.sessionid""",
          
          # 있는 코드인지 체크하여 count 할 수 있다, TIMESTAMP를 사용하려면 WHERE 을 써서 구체적으로 만들 수 있다
          'input_check':
          [
            {
              'sql': 'SELECT COUNT(1) FROM raw_data.user_session_channel',
              'count': 101000
            },
          ],          
          
          # 최종 sql을 보내주기 위함
          'output_check':
          [
            {
              'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
              'count': 101000
            }
          ],
}
