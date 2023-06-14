from streamingTfLDataProject.Producer.pyspark_producer import stream_data
import threading


# bus_arrivals
bus_arrival_topic = 'TfL-bus-arrival'
bus_arrival_url = "https://api.tfl.gov.uk/Stoppoint?lat=51.4929&lon=" \
      "0.053929&stoptypes=NaptanBusCoachStation," \
      "NaptanPublicBusCoachTram"
# stream_data(bus_arrival_topic, bus_arrival_url)

#crowding
crowding_topic = 'TfL-crowding'
crowding_url = 'https://api.tfl.gov.uk/crowding/940GZZLUBND'

#
valid_severity_topic = 'TfL-severity'
valid_severity_url = 'https://api.tfl.gov.uk/Line/Meta/Severity'

# Create and start threads for each invocation
thread_bus_arrival = threading.Thread(target=stream_data, args=(bus_arrival_topic, bus_arrival_url))
thread_crowding = threading.Thread(target=stream_data, args=(crowding_topic, crowding_url))
valid_severity = threading.Thread(target=stream_data, args=(valid_severity_topic, valid_severity_url))

thread_bus_arrival.start()
thread_crowding.start()
valid_severity.start()

# Wait for both threads to complete
thread_bus_arrival.join()
thread_crowding.join()
valid_severity.join()