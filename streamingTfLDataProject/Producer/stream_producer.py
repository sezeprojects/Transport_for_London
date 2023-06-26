from Transport_for_London.streamingTfLDataProject.Producer.pyspark_producer import stream_data
import threading


naptan_ids = ('490001345A', '490011886W', '490006983DE', '490011247DC', '490006982DG', '490001345B')
naptan_id_topics = ('stop-DA', 'stop-DD', 'stop-DE', 'stop-DC', 'stop-DG', 'stop-DB')

# bus_arrivals
bus_arrival_topic = 'TfL-bus-arrival'
bus_arrival_url = "https://api.tfl.gov.uk/Stoppoint?lat=51.4929&lon=" \
                  "0.053929&stoptypes=NaptanBusCoachStation," \
                  "NaptanPublicBusCoachTram"

# crowding
crowding_topic = 'TfL-crowding'
crowding_url = 'https://api.tfl.gov.uk/crowding/940GZZLUBND'

# severity
valid_severity_topic = 'TfL-severity'
valid_severity_url = 'https://api.tfl.gov.uk/Line/Meta/Severity'


def stream_stop_time(naptan_id, naptan_id_topic):
    bus_time_url = f"https://api.tfl.gov.uk/StopPoint/{naptan_id}/Arrivals?mode=bus"
    bus_time = stream_data(naptan_id_topic, bus_time_url)
    return bus_time


def start_stream_data(topic, url):
    stream_data(topic, url)


if __name__ == '__main__':
    # Start the remaining threads
    threads = []

    # Start bus_arrival process
    bus_arrival_thread = threading.Thread(target=start_stream_data, args=(bus_arrival_topic, bus_arrival_url))
    bus_arrival_thread.start()
    threads.append(bus_arrival_thread)

    # Start crowding process
    crowding_thread = threading.Thread(target=start_stream_data, args=(crowding_topic, crowding_url))
    crowding_thread.start()
    threads.append(crowding_thread)

    # Start valid_severity process
    valid_severity_thread = threading.Thread(target=start_stream_data, args=(valid_severity_topic, valid_severity_url))
    valid_severity_thread.start()
    threads.append(valid_severity_thread)

    # Create and start threads for each naptan_id and naptan_topic pair
    for bus_naptan_id, nap_id_topic in zip(naptan_ids, naptan_id_topics):
        process = threading.Thread(target=stream_stop_time, args=(bus_naptan_id, nap_id_topic))
        process.start()
        threads.append(process)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()
