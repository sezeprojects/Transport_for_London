from streamingTfLDataProject.Producer.pyspark_producer import stream_data


# bus_arrivals
bus_arrival_topic = 'TfL-bus-arrival'
bus_arrival_url = "https://api.tfl.gov.uk/Stoppoint?lat=51.4929&lon=" \
      "0.053929&stoptypes=NaptanBusCoachStation," \
      "NaptanPublicBusCoachTram"
stream_data(bus_arrival_topic, bus_arrival_url)

#crowding
crowing_topic = 'TfL-crowding'
