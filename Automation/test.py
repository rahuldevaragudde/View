
i=1
while(i==1):
    import requests
    token = 'Bearer tok_Q05xICN1RhWEm9qDf3vljHeJYRI5i4NBrams5JYTCEW'
    start_time = '2019-07-24T00:00:00Z'
    end_time = '2019-07-25T18:00:00Z'
    period = '30m'
    pagesize = '10'
    JARVIS = 'spc_685547081896559368'

    #list spaces
    r = requests.get('https://api.density.io/v2/spaces/', headers={'Authorization': token})
    output = r.json()
    results = output['results']
    for id in results:
      density_id = id.get('id')
      density_name = id.get('name')
      print(density_id, density_name)

    #space count for JARVIS
    r = requests.get('https://api.density.io/v2/spaces/' + JARVIS + '/counts/\
    ?start_time=' + start_time + '&end_time=' + end_time + '&interval=' + period + '&page_size=' + pagesize, headers={'Authorization': token})
    output = r.json()
    results = output['results']
    for ts in results:
      timestamp = ts.get('timestamp')
      interval = ts.get('interval')
      analytics = interval.get('analytics')
      print(timestamp, analytics.get('max'))

    while 1:
      next_page = output['next']
      if next_page is None:
        print ('This is last page')
        break
      else:
        print (next_page)
        r = requests.get(next_page, headers={'Authorization': token})
        output = r.json()
        results = output['results']
        for ts in results:
          timestamp = ts.get('timestamp')
          interval = ts.get('interval')
          analytics = interval.get('analytics')
          print(timestamp, analytics.get('max'))
    time.sleep(10)
